package main

import (
	"github.com/google/uuid"
	"time"
)

type Signature struct {
	id *string // no json representation
	Name    string `json:"name"`
	ShortName    string `json:"shortname"`
	Signature string `json:"signature"`
	Signature2 string `json:"signature2"`
	Size int64 `json:"size"`
	Time time.Time `json:"time"`
	Archive bool `json:"archive"`
}

func (s *Signature) setId(id string) {
	s.id = &id
}

func (s *Signature) getIdOrUUID() string {
	if s.id != nil {
		return *s.id
	} else {
		return uuid.NewString()
	}
}

