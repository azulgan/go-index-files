package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func move(a EsInterface, filemover FileMover, c *Config, date string) error {
	list := a.LoadAllByNameMatch(date, 10000)
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
				err := filemover.MoveSecurely(a, v, newpath)
				if err != nil {
					return err
				}
			}
		} else {
			//log.Println(v.Name, " ignored")
		}
	}
	filemover.EndReport()
	return nil
}

func MoveFile(sourcePath, destPath string) error {
	os.MkdirAll(filepath.Dir(destPath), os.ModePerm)
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
