package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/olivere/elastic/v7"
	"log"
)

type EsInterface interface {
	CreateIndexIfNecessary()
	LoadAllByNameMatch(searchedName string, max int) []Signature
	LoadByPath(searchedName string) *Signature
	SignatureDuplicates() *[][]Signature
	InsertBulk(signatures []Signature, max int) error
	DeleteById(id string, name string) error
	SavePathChange(v Signature, newpath string) error
}

type EsAdapter struct {
	client *elastic.Client
	index string
}

func NewEsAdapter(client *elastic.Client, index string) EsInterface {
	return &EsAdapter{client: client, index: index}
}

func (a *EsAdapter) CreateIndexIfNecessary() {
	indexExists, err := a.client.IndexExists(a.index).Do(context.Background())
	if err != nil {
		panic(err)
	}
	if !indexExists {
		_, err = a.client.CreateIndex(a.index).Do(context.Background())
		if err != nil {
			// Handle error
			panic(err)
		}
	}
}

func (a *EsAdapter) load(searchedName string, max int) []Signature {
	return a.loadAll(searchedName, "signature.keyword", max)
}

func (a *EsAdapter) LoadAllByNameMatch(searchedName string, max int) []Signature {
	ret := make([]Signature, 0)
	const SLICES = 1000
	fromIdx := 0
	for (max > SLICES) {
		ret = append(ret, a.loadAllMatch(searchedName, "shortname", fromIdx, SLICES)...)
		fromIdx += SLICES
		max -= SLICES
	}
	ret = append(ret, a.loadAllMatch(searchedName, "shortname", fromIdx, max)...)
	return ret
}

func (a *EsAdapter) loadAll(searchedName string, field string, max int) []Signature {
	// Search with a term query
	termQuery := elastic.NewTermQuery(field, searchedName)
	searchResult, err := a.client.Search().
		Index(a.index).            // exists in index "tweets"
		Query(termQuery).           // specify the query
		Sort("shortname.keyword", true). // sort by "user" field, ascending
		From(0).Size(max).           // take documents 0-9
		//Pretty(true).               // pretty print request and response JSON
		Do(context.Background())    // execute
	if err != nil {
		// Handle error
		panic(err)
	}

	ret := []Signature{}
	if searchResult.Hits.TotalHits.Value > 0 {
		//fmt.Printf("Found a total of %d tweets\n", searchResult.Hits.TotalHits)
		//
		//// Iterate through results
		for _, hit := range searchResult.Hits.Hits {
			// Deserialize hit.Source into a Tweet (could also be just a map[string]interface{}).
			var t Signature
			//err := json.Unmarshal(*hit.Source, &t)
			err := json.Unmarshal(hit.Source, &t)
			t.setId(hit.Id)
			if err != nil {
				// Deserialization failed
			}
			ret = append(ret, t)
		}
	}
	return ret
}

func (a *EsAdapter) loadAllMatch(searchedName string, field string, fromIdx int, max int) []Signature {
	// Search with a term query
	termQuery := elastic.NewQueryStringQuery(field + ": '" + searchedName + "'")
	searchResult, err := a.client.Search().
		Index(a.index).            // exists in index "tweets"
		Query(termQuery).           // specify the query
		//Sort("shortname.keyword", true). // sort by "user" field, ascending
		From(fromIdx).Size(max).
		Pretty(true).               // pretty print request and response JSON
		Do(context.Background())    // execute
	if err != nil {
		// Handle error
		panic(err)
	}

	ret := []Signature{}
	if searchResult.Hits.TotalHits.Value > 0 {
		//fmt.Printf("Found a total of %d tweets\n", searchResult.Hits.TotalHits)
		//
		//// Iterate through results
		for _, hit := range searchResult.Hits.Hits {
			// Deserialize hit.Source into a Tweet (could also be just a map[string]interface{}).
			var t Signature
			//err := json.Unmarshal(*hit.Source, &t)
			err := json.Unmarshal(hit.Source, &t)
			t.setId(hit.Id)
			if err != nil {
				// Deserialization failed
			}
			ret = append(ret, t)
		}
	}
	return ret
}

func (a *EsAdapter) LoadByPath(searchedName string) *Signature {
	// Search with a term query
	termQuery := elastic.NewTermQuery("name.keyword", searchedName)
	searchResult, err := a.client.Search().
		Index(a.index).            // exists in index "tweets"
		Query(termQuery).           // specify the query
		//Sort("story", true). // sort by "user" field, ascending
		From(0).Size(1).           // take documents 0-9
		//Pretty(true).               // pretty print request and response JSON
		Do(context.Background())    // execute
	if err != nil {
		// Handle error
		panic(err)
	}

	// searchResult is of type SearchResult and returns hits, suggestions,
	// and all kinds of other information from Elasticsearch.
	//fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)

	// Each is a convenience function that iterates over hits in a exists result.
	// It makes sure you don't need to check for nil values in the response.
	// However, it ignores errors in serialization. If you want full control
	// over iterating the hits, see below.
	//var ttyp Story
	//for _, item := range searchResult.Each(reflect.TypeOf(ttyp)) {
	//	if t, ok := item.(Story); ok {
	//		fmt.Printf("Tweet by %s: %s\n", t.Name, t.Message)
	//	}
	//}
	//// TotalHits is another convenience function that works even when something goes wrong.
	//fmt.Printf("Found a total of %d tweets\n", searchResult.TotalHits())

	// Here's how you iterate through results with full control over each step.
	if searchResult.Hits.TotalHits.Value > 0 {
		//fmt.Printf("Found a total of %d tweets\n", searchResult.Hits.TotalHits)
		//
		//// Iterate through results
		for _, hit := range searchResult.Hits.Hits {
			// hit.Index contains the name of the index

			// Deserialize hit.Source into a Tweet (could also be just a map[string]interface{}).
			var t Signature
			//err := json.Unmarshal(*hit.Source, &t)
			err := json.Unmarshal(hit.Source, &t)
			t.setId(hit.Id)
			if err != nil {
				// Deserialization failed
			}
			return &t
		}
	}
	return nil
}

func (a *EsAdapter) SignatureDuplicates() *[][]Signature {
	// Search with a term query
	ret := [][]Signature{}
	//termQuery := elastic.NewTermQuery("author.keyword", searchedName)
	termQuery := elastic.NewMatchAllQuery()
	termsAggreg := elastic.NewTermsAggregation().Field("signature.keyword").Size(100000).MinDocCount(2)
	searchResult, err := a.client.Search().
		Index(a.index).            // exists in index "tweets"
		Query(termQuery).           // specify the query
		//Sort("name.keyword", true). // sort by "user" field, ascending
		From(0).Size(0).           // take documents 0-9
		//Pretty(true).               // pretty print request and response JSON
		Aggregation("duplSignatures", termsAggreg).
		Do(context.Background())    // execute
	if err != nil {
		// Handle error
		panic(err)
	}
	termRes, present := searchResult.Aggregations.Terms("duplSignatures")
	if present && len(termRes.Buckets) > 0 {

		//fmt.Printf("Found a total of %d tweets\n", searchResult.Hits.TotalHits)
		list := []string{}
		//// Iterate through results
		for _, hit := range termRes.Buckets {
			// hit.Index contains the name of the index
			//hit.KeyAsString
			var key = hit.Key.(string)
			if key != "" {
				list = append(list, key)
			}
		}
		for _, key := range list {
			ret = append(ret, a.load(key, 10))
		}
	}
	return &ret
}

func (a *EsAdapter) insertByChuncks(stories []Signature, chucksSize int) error {
	var storiesChuncks = a.split(stories, chucksSize)
	for _, ch := range storiesChuncks {
		err := a.InsertBulk(ch, len(stories))
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *EsAdapter) split(stories []Signature, chunkSize int) [][]Signature {
	ret := [][]Signature{}
	for i := 0; i < len(stories); i += chunkSize {
		end := i + chunkSize
		if end > len(stories) {
			end = len(stories)
		}
		ret = append(ret, stories[i:end])
	}
	return ret
}

func (a *EsAdapter) InsertBulk(signatures []Signature, max int) error {
	bulk := a.client.Bulk()
	atLeastOne := false
	for i := 0; i < max; i++ {
		signature := signatures[i]
		req := elastic.NewBulkIndexRequest()
		req.OpType("index") // set type to "index" document
		req.Index(a.index)
		//req.Type("_doc") // Doc types are deprecated (default now _doc)
		idStr := signature.getIdOrUUID()
		req.Id(idStr)
		req.Doc(signature)
		//fmt.Println("req TYPE:", reflect.TypeOf(req))
		bulk = bulk.Add(req)
		fmt.Println("NewBulkIndexRequest().Insert: ", idStr)
		atLeastOne = true
	}
	if atLeastOne {
		bulkResp, err := bulk.Do(context.Background())

		// Check if the Do() method returned any errors
		if err != nil {
			log.Fatalf("bulk.Do(ctx) ERROR: %s", err)
		} else {
			//// If there is no error then get the Elasticsearch API response
			indexed := bulkResp.Indexed()
			//fmt.Println("nbulkResp.Indexed():", indexed)
			//fmt.Println("bulkResp.Indexed() TYPE:", reflect.TypeOf(indexed))
			//
			//// Iterate over the bulkResp.Indexed() object returned from bulk.go
			//t := reflect.TypeOf(indexed)
			//fmt.Println("nt:", t)
			//fmt.Println("NewBulkIndexRequest().NumberOfActions():", bulk.NumberOfActions())
			//
			//// Iterate over the document responses
			//for i := 0; i < t.NumMethod(); i++ {
			//	method := t.Method(i)
			//	fmt.Println("nbulkResp.Indexed() METHOD NAME:", i, method.Name)
			//	fmt.Println("bulkResp.Indexed() method:", method)
			//}
			//
			//// Return data on the documents indexed
			//fmt.Println("nBulk response Index:", indexed)
			for _, info := range indexed {
				if info.Status == 429 {
					return errors.New(info.Error.Reason)
				}
				//fmt.Println("nBulk response Index:", info)
				//fmt.Println("nBulk response Index:", info.Index)
			}
		}
	}
	return nil
}

func (a *EsAdapter) DeleteById(id string, name string) error {
	ctx := context.Background()
	res, err := a.client.Delete().
		Index(a.index).
		Id(id).
		Do(ctx)
	if err != nil {
		return err
	}
	if res.Status == 0 {
		fmt.Println("Document deleted from from index: ", name)
	}
	return nil
}

func (a *EsAdapter) saveSingleSignature(v Signature) error {
	signatures := make([]Signature, 1)
	signatures[0] = v
	return a.insertByChuncks(signatures, 1)
}

func (a *EsAdapter) SavePathChange(v Signature, newpath string) error {
	v.Name = newpath
	return a.saveSingleSignature(v)
}
