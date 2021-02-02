package main

import (
	"context"
	"encoding/json"
	"github.com/olivere/elastic/v7"
)

func exists(client *elastic.Client, searchedName string) bool {
	// Search with a term query
	termQuery := elastic.NewTermQuery("name.keyword", searchedName)
	searchResult, err := client.Search().
		Index("story").            // exists in index "tweets"
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
		//for _, hit := range searchResult.Hits.Hits {
		//	// hit.Index contains the name of the index
		//
		//	// Deserialize hit.Source into a Tweet (could also be just a map[string]interface{}).
		//	var t Story
		//	//err := json.Unmarshal(*hit.Source, &t)
		//	err := json.Unmarshal(hit.Source, &t)
		//	if err != nil {
		//		// Deserialization failed
		//	}
		//
		//	// Work with tweet
		//	fmt.Printf("Tweet by %s: %s\n", t.Name, t.Message)
		//}
		return true
	} else {
		// No hits
		//fmt.Print("Found no tweets\n")
		return false
	}

}

func load(client *elastic.Client, index string, searchedName string) *Story {
	// Search with a term query
	termQuery := elastic.NewTermQuery("name.keyword", searchedName)
	searchResult, err := client.Search().
		Index(index).            // exists in index "tweets"
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
			var t Story
			//err := json.Unmarshal(*hit.Source, &t)
			err := json.Unmarshal(hit.Source, &t)
			if err != nil {
				// Deserialization failed
			}
			return &t
		}
	}
	return nil
}

func pageByAuthor(client *elastic.Client, index string, searchedName string) []Story {
	// Search with a term query
	ret := []Story{}
	//termQuery := elastic.NewTermQuery("author.keyword", searchedName)
	termQuery := elastic.NewTermQuery("author.keyword", searchedName)
	searchResult, err := client.Search().
		Index(index).            // exists in index "tweets"
		Query(termQuery).           // specify the query
		//Sort("name.keyword", true). // sort by "user" field, ascending
		From(0).Size(1000).           // take documents 0-9
		//Pretty(true).               // pretty print request and response JSON
		Do(context.Background())    // execute
	if err != nil {
		// Handle error
		panic(err)
	}

	if searchResult.Hits.TotalHits.Value > 0 {
		//fmt.Printf("Found a total of %d tweets\n", searchResult.Hits.TotalHits)
		//
		//// Iterate through results
		for _, hit := range searchResult.Hits.Hits {
			// hit.Index contains the name of the index

			// Deserialize hit.Source into a Tweet (could also be just a map[string]interface{}).
			var t Story
			//err := json.Unmarshal(*hit.Source, &t)
			err := json.Unmarshal(hit.Source, &t)
			if err != nil {
				// Deserialization failed
			}
			ret = append(ret, t)
		}
	}
	return ret
}

func pageByKeyword(client *elastic.Client, index string, searchedName string) []Story {
	// Search with a term query
	ret := []Story{}
	//termQuery := elastic.NewTermQuery("author.keyword", searchedName)
	termQuery := elastic.NewTermQuery("message", searchedName)
	searchResult, err := client.Search().
		Index(index).            // exists in index "tweets"
		Query(termQuery).           // specify the query
		//Sort("name.keyword", true). // sort by "user" field, ascending
		From(0).Size(1000).           // take documents 0-9
		//Pretty(true).               // pretty print request and response JSON
		Do(context.Background())    // execute
	if err != nil {
		// Handle error
		panic(err)
	}

	if searchResult.Hits.TotalHits.Value > 0 {
		//fmt.Printf("Found a total of %d tweets\n", searchResult.Hits.TotalHits)
		//
		//// Iterate through results
		for _, hit := range searchResult.Hits.Hits {
			// hit.Index contains the name of the index

			// Deserialize hit.Source into a Tweet (could also be just a map[string]interface{}).
			var t Story
			//err := json.Unmarshal(*hit.Source, &t)
			err := json.Unmarshal(hit.Source, &t)
			if err != nil {
				// Deserialization failed
			}
			ret = append(ret, t)
		}
	}
	return ret
}
