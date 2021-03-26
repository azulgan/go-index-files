package main

import (
	"fmt"
	"github.com/olivere/elastic/v7"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Config struct {
	Incoming struct {
		Folder string `yaml:"folder" default:"."`
		Extension string `yaml:"extension" default:".jpeg"`
		NameFormat string `yaml:"nameFormat" default:"2006-01-02T150405.jpg"`
	} `yaml:"incoming"`
	Walker struct {
		Folder1 string `yaml:"folder1" default:"."`
		Folder2 string `yaml:"folder2" default:"."`
		Extension string `yaml:"extension" default:".jpg"`
	} `yaml:"walker"`
	Es struct {
		Index string `yaml:"index" default:"signatures"`
		BulkInsert int `yaml:"bulkInsert" default:100"`
	} `yaml:"es"`
	Duplicates struct {
		Action string `yaml:"action" default:"show"`
	} `yaml:"duplicates"`
}

func loadConfig() Config {
	f, err := os.Open("Config.yaml")
	if err != nil {
		log.Fatalf("Open Config ERROR: %s", err)
	}
	defer f.Close()

	var cfg Config
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&cfg)
	if err != nil {
		log.Fatalf("Decode Config ERROR: %s", err)
	}
	return cfg
}

func nextParam(args []string) ([]string, string) {
	var ret string
	ret = ""
	if len(args) > 0 {
		ret = args[0]
		args = args[1:]
	}
	return args, ret
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
	adapter := NewEsAdapter(client, config.Es.Index)
	adapter.CreateIndexIfNecessary()
	action := ""
	args := os.Args[1:]
	for len(args) > 0 {
		args, action = nextParam(args)

		if action == "index" {
			folder := config.Walker.Folder1
			//if len(args) > 0 {
			//	folder = args[0]
			//}
			walk(adapter, &config, folder, true)
			folder = config.Walker.Folder2
			walk(adapter, &config, folder, false)
		} else if action == "scan" {
			fmt.Println("Scanning directories...")
			folder := config.Walker.Folder2
			walk(adapter, &config, folder, false)
			fmt.Println("done")
		} else if action == "dupl" {
			fmt.Println("Chasing duplicates...")
			dupls(adapter, &config, false)
			fmt.Println("done")
		} else if action == "web" {
			web(client, &config)
		} else if action == "move" {
			theDate := ""
			args, theDate = nextParam(args)
			err := move(adapter, NewFileMover(false), &config, theDate)
			if err != nil {
				log.Panic(err)
			}
		} else if action == "movedryrun" {
			theDate := ""
			args, theDate = nextParam(args)
			err := move(adapter, NewFileMover(true), &config, theDate)
			if err != nil {
				log.Panic(err)
			}
		} else if action == "incoming" {
			fmt.Println("Looking for incoming files...")
			err := incoming(&config)
			if err != nil {
				log.Panic(err)
			}
			fmt.Println("done")
		} else if action == "wait" {
			fmt.Println("Waiting 3 seconds...")
			time.Sleep(3 * time.Second)
			fmt.Println("done")
		}
	}
	//exists(client, "")
}

func goodFile(f os.FileInfo, c *Config) bool {
	return !f.IsDir() && strings.HasSuffix(f.Name(), c.Incoming.Extension)
}

func incoming(c *Config) error {
	o := NewOsHandler()
	files, err := ioutil.ReadDir(c.Incoming.Folder)
	if err != nil {
		return err
	}

	for _, f := range files {
		if goodFile(f, c) {
			curFileName := filepath.Join(c.Incoming.Folder, f.Name())
			fmt.Println(f.Name())
			newName := f.ModTime().Format(c.Incoming.NameFormat)
			folder2 := newName[0:10]
			folder1 := folder2[0:7]
			newPath := filepath.Join(c.Walker.Folder2, folder1, folder2, newName)
			moveOrRenameAndMove(o, curFileName, newPath)
		}
	}
	return nil
}


func moveOrRenameAndMove(o OsInterface, f string, path string) {
	o.EnsureDirExists(path)
	count := 0
	ext := filepath.Ext(path)
	start := path[0:len(path)-len(ext)]
	newPath := path
	for o.ExistsFile(newPath) {
		count++
		newPath = start + "_" + fmt.Sprint(count) + ext
	}
	o.MoveFile(f, newPath)
}

