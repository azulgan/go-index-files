package main

import "time"

type Story struct {
	Name    string `json:"name"`
	Message string `json:"message"`
	Title   string `json:"title"`
	Author   string `json:"author"`
	ModTime time.Time `json:"modtime"`
}


