package main

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	"gopkg.in/yaml.v2"
	"log"
	"os"
)

type Config struct {
	Walker struct {
		Folder string `yaml:"folder"`
	} `yaml:"walker"`
	Es struct {
		Index string `yaml:"index"`
	} `yaml:"es"`
}

func loadConfig() Config {
	f, err := os.Open("Config.yaml")
	if err != nil {
		log.Fatalf("Open Config ERROR:", err)
	}
	defer f.Close()

	var cfg Config
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&cfg)
	if err != nil {
		log.Fatalf("Decode Config ERROR:", err)
	}
	return cfg
}

func main() {
	config := loadConfig()
	// Create a client
	client, err := elastic.NewSimpleClient(elastic.SetURL("http://localhost:9200"))
	if err != nil {
		// Handle error
		fmt.Printf("Connection %s\n", err)
		os.Exit(3)

	}

	indexExists, err := client.IndexExists("story").Do(context.TODO())
	if err != nil {
		panic(err)
	}
	if !indexExists {
		_, err = client.CreateIndex("story").Do(context.Background())
		if err != nil {
			// Handle error
			panic(err)
		}
		//_, err = client.DeleteIndex("story").Do(context.Background())
		//if err != nil {
		//	// Handle error
		//	panic(err)
		//}
	}

	action := ""
	args := os.Args[1:]
	if len(args) > 0 {
		action = args[0]
		args = args[1:]
	}
	if config.Es.Index == "" {
		config.Es.Index = "defaultStory"
	}
	if action == "index" {
		folder := config.Walker.Folder
		if len(args) > 0 {
			folder = args[0]
		}
		walk(client, &config, folder)
	} else if action == "web" {
		web(client, &config)
	}
	//exists(client, "")

}

