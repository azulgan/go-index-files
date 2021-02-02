package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/antchfx/htmlquery"
	"github.com/olivere/elastic/v7"
	"golang.org/x/net/html"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	//"reflect"
	"regexp"
	"strconv"
	"strings"
)

func walk(client *elastic.Client, index *Config, folder string) {
	stories := []Story{}
	err := filepath.Walk(folder,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if isGoodFile(info) {
				s := load(client, index, info.Name())
				if s == nil {
					dat, _ := ioutil.ReadFile(path)
					//fmt.Print(string(dat))
					//datstr := string(dat)
					htmlParsed := openAndFilter(dat)
					title := getTitleFromDoc(htmlParsed)
					datstr := asString(htmlParsed)
					author := parseAuthor(title)
					fmt.Println(author, info.Size())
					story := Story{Name: info.Name(), Message: datstr, Title: title, Author: author, ModTime: info.ModTime()}
					stories = append(stories, story)
				} else {
					fmt.Println("Ignoring ", s.Name)
					//fmt.Print("Test: ", s.Author, " ", s.Title, " ", s.Name)
					//if s.Author == "" || s.Title == "" || s.Title == "title" {
					//	datstr := s.Message
					//	htmlParsed := openAndFilter([]byte(datstr))
					//	title := getTitleFromDoc(htmlParsed)
					//	s.Message = asString(htmlParsed)
					//	s.Title = title
					//	s.Author = parseAuthor(s.Title)
					//	fmt.Println("Reassigning title and author ", s.Title, s.Name)
					//	_, err = client.Index().
					//		Index("story").
					//		Type("doc").
					//		//Id("1").
					//		BodyJson(s).
					//		Refresh("wait_for").
					//		Do(context.Background())
					//}
				}
			}
			return nil
		})
	if err != nil {
		log.Println(err)
	}
	insertByChuncks(client, index, stories, 100)

}

func insertByChuncks(client *elastic.Client, index string, stories []Story, chucksSize int) {
	var storiesChuncks = split(stories, chucksSize)
	for i, ch := range storiesChuncks {
		insertBulk(client, index, ch, i * chucksSize)
	}
}

func split(stories []Story, chunkSize int) [][]Story {
	ret := [][]Story{}
	for i := 0; i < len(stories); i += chunkSize {
		end := i + chunkSize

		if end > len(stories) {
			end = len(stories)
		}

		ret = append(ret, stories[i:end])
	}
	return ret
}

func insertBulk(client *elastic.Client, index string, stories []Story, startId int) {
	bulk := client.Bulk()
	docID := startId
	for _, story := range stories {

		// Incrementally change the _id number in each iteration
		docID++

		// Convert the _id integer into a string
		idStr := strconv.Itoa(docID)
		req := elastic.NewBulkIndexRequest()
		req.OpType("index") // set type to "index" document
		req.Index(index)
		//req.Type("_doc") // Doc types are deprecated (default now _doc)
		req.Id(idStr)
		req.Doc(story)
		//fmt.Println("req:", req)
		//fmt.Println("req TYPE:", reflect.TypeOf(req))
		bulk = bulk.Add(req)
		fmt.Println("NewBulkIndexRequest().NumberOfActions():", bulk.NumberOfActions())
	}
	_, err := bulk.Do(context.Background())

	// Check if the Do() method returned any errors
	if err != nil {
		log.Fatalf("bulk.Do(ctx) ERROR:", err)
	} else {
		//// If there is no error then get the Elasticsearch API response
		//indexed := bulkResp.Indexed()
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
		//for _, info := range indexed {
		//	fmt.Println("nBulk response Index:", info)
		//	//fmt.Println("nBulk response Index:", info.Index)
		//}
	}
}

func parseAuthor(title string) string {
	re := regexp.MustCompile("^([^\\']*)'")
	match := re.FindStringSubmatch(title)
	if match == nil {
		if len(title) >= 9 && title[0:9] == "Anonymous" {
			return title[0:9]
		} else {
			return title
		}
	}
	return match[1]
}

func openAndFilter(data []byte) *html.Node {
	doc, err := htmlquery.Parse(bytes.NewReader(data))
	if err != nil {
		return nil
	}
	script, err := htmlquery.QueryAll(doc, "//script")
	for _ , s := range script {
		for s.FirstChild != nil {
			s.RemoveChild(s.FirstChild)
		}
		found := -1
		for i, a := range s.Attr {
			if a.Key == "src" {
				found = i
			}
		}
		if found != -1 {
			s.Attr[found].Val = ""
		}
	}
	return doc
}

func asString(doc *html.Node) string {
	return htmlquery.OutputHTML(doc, true)
}

func getTitleFromDoc(doc *html.Node) string {
	node, err := htmlquery.Query(doc, "//title")
	if err != nil {
		return ""
	}
	//script, err := htmlquery.QueryAll(doc, "//script")
	//htmlquery.OutputHTML(doc, true)
	if node != nil {
		return node.FirstChild.Data
	}
	return ""
}

func getTitle(datstr []byte) string {
	doc, err := htmlquery.Parse(bytes.NewReader(datstr))
	node, err := htmlquery.Query(doc, "//title")
	if err != nil {
		return ""
	}
	//script, err := htmlquery.QueryAll(doc, "//script")
	//htmlquery.OutputHTML(doc, true)
	if node != nil {
		return node.FirstChild.Data
	}
	return ""
}

func isGoodFile(info os.FileInfo) bool {
	return !info.IsDir() && strings.HasSuffix(info.Name(), ".html")
}
