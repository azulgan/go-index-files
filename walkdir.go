package main

import (
	"crypto/md5"
	"crypto/sha1"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"

	//"strconv"
	"strings"
)

func walk(adapter *EsAdapter, config *Config, folder string, archive bool) {
	startTime := time.Now()
	cur := 0
	signatureChan := make(chan Signature)
	endSignalChan := make(chan bool)
	go inserter(adapter, signatureChan, endSignalChan, config, startTime)
	//refDate := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	err := filepath.Walk(folder,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if isGoodFile(info, config) {
				s := adapter.loadByPath(path)
				if s == nil {
					if (cur / 10) * 10 == cur {
						fmt.Print("Scanning ", path, "... ")
					}
					dat, _ := ioutil.ReadFile(path)
					md5Sum := fmt.Sprintf("%x", md5.Sum(dat))
					sha1Sum := fmt.Sprintf("%x", sha1.Sum(dat))
					story := Signature{Name: path, ShortName: info.Name(), Signature: md5Sum,
						Signature2: sha1Sum,
						Size: info.Size(), Time: info.ModTime(), Archive: archive}
					signatureChan <- story
					if (cur / 10) * 10 == cur {
						fmt.Println("Done")
					}
					cur++
				} else {
					if s.ShortName == "" {
						s.ShortName = info.Name()
						s.Archive = archive
						signatureChan <- *s
					//} else {
					//	//fmt.Println("Ignoring ", s.Name)
					}
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
	close(signatureChan)
	fmt.Println("Precalc duration : ", time.Now().Sub(startTime))
	endSignal := <- endSignalChan
	fmt.Println("Ended coroutine with signal ", endSignal)
}

func isGoodFile(info os.FileInfo, config *Config) bool {
	return !info.IsDir() && strings.HasSuffix(info.Name(), config.Walker.Extension)
}

func inserter(adapter *EsAdapter, ch chan Signature, signalChan chan bool,
	config *Config, startTime time.Time) {
	for {
		max := config.Es.BulkInsert
		var ok bool
		var latestIndex = -1
		sigArray := make([]Signature, max)
		for i := 0; i < max; i++ {
			sigArray[i], ok = <- ch
			if !ok {
				break
			}
			latestIndex = i
		}
		err := adapter.insertBulk(sigArray, latestIndex + 1)
		if (!ok) {
			break
		}
		if err != nil {
			fmt.Println("Something went wrong ", err)
			panic(err)
			break
		}
	}
	fmt.Println("Full duration : ", time.Now().Sub(startTime))
	signalChan <- true
}