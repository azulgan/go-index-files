package main

import (
	"fmt"
	"github.com/olivere/elastic/v7"
	"gopkg.in/yaml.v2"
	"io"
	"log"
	"os"
	"path/filepath"
)

type Config struct {
	Walker struct {
		Folder1 string `yaml:"folder1" default:"."`
		Folder2 string `yaml:"folder2" default:"."`
		Extension string `yaml:"extension" default:".jpg"`
	} `yaml:"walker"`
	Es struct {
		Index string `yaml:"index" default:"story"`
		BulkInsert int `yaml:"bulkInsert" default:100"`
	} `yaml:"es"`
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
	adapter.createIndexIfNecessary()
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
			folder := config.Walker.Folder2
			walk(adapter, &config, folder, false)
		} else if action == "dupl" {
			dupls(adapter, false)
		} else if action == "web" {
			web(client, &config)
		} else if action == "move" {
			theDate := ""
			args, theDate = nextParam(args)
			err := move(adapter, &config, theDate)
			if err != nil {
				panic(err)
			}
		}
	}
	//exists(client, "")
}

func move(a *EsAdapter, c *Config, date string) error {
	list := a.loadAllByNameMatch(date, 10000)
	basedir := c.Walker.Folder2
	targetdir := c.Walker.Folder1
	for _, v := range list {
		if v.Name[0:len(basedir)] == basedir {
			startdatepos := len(basedir) + 9
			datestr := v.Name[startdatepos:startdatepos+10]
			if datestr != date {
				//log.Println(v.Name, " suspect: ", datestr)
				// checked the correctness of the date. Since we use Elasticsearch to have a fast search,
				// especially the 'Match' search, the results have to be filtered.
			} else {
				fileandfolder := v.Name[len(basedir) + 1:]
				newpath := filepath.Join(targetdir, fileandfolder)
				os.MkdirAll(filepath.Dir(newpath), os.ModePerm)
				err := MoveFile(v.Name, newpath)
				if err != nil {
					log.Panic(err)
				} else {
					v.Name = newpath
					err := a.saveSingleSignature(v)
					if err != nil {
						MoveFile(newpath, v.Name)
						return err
					}
					log.Println(v.Name, " treated into ", newpath)
				}
			}
		} else {
			//log.Println(v.Name, " ignored")
		}
	}
	return nil
}

func MoveFile(sourcePath, destPath string) error {
	inputFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("Couldn't open source file: %s", err)
	}
	outputFile, err := os.Create(destPath)
	if err != nil {
		inputFile.Close()
		return fmt.Errorf("Couldn't open dest file: %s", err)
	}
	defer outputFile.Close()
	_, err = io.Copy(outputFile, inputFile)
	inputFile.Close()
	if err != nil {
		return fmt.Errorf("Writing to output file failed: %s", err)
	}
	// The copy was successful, so now delete the original file
	err = os.Remove(sourcePath)
	if err != nil {
		return fmt.Errorf("Failed removing original file: %s", err)
	}
	return nil
}
