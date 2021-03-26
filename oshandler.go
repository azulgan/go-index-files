package main

import (
	"os"
	"path/filepath"
)

type OsInterface interface {
	ExistsFile(path string) bool
	EnsureDirExists(path string)
	MoveFile(filePath string, newPath string)
}

type OsHandler struct {

}

func NewOsHandler() OsInterface {
	return OsHandler{}
}

func (o OsHandler) ExistsFile(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func (o OsHandler) EnsureDirExists(path string) {
	os.MkdirAll(filepath.Dir(path), os.ModePerm)
}

func (o OsHandler) MoveFile(filePath string, newPath string) {
	MoveFile(filePath, newPath) // no need to recreate it from scratch now
}