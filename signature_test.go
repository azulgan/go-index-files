package main

import (
	"testing"
)

func TestId(t *testing.T) {
	want := "Hello, world."
	s := Signature{}
	s.setId(want)
	if got := s.getIdOrUUID(); got != want {
		t.Errorf("getIdOrUUID = %q, want %q", got, want)
	}
}


func TestUUID(t *testing.T) {
	want := "Hello, world."
	s := Signature{}
	if got := s.getIdOrUUID(); got == want {
		t.Errorf("getIdOrUUID = %q, want something else", got)
	}
	got := s.getIdOrUUID();
	if len(got) != 36 {
		t.Errorf("getIdOrUUID = %q, want 36 chars", got)
	}
	if newgot := s.getIdOrUUID(); got == newgot {
		t.Errorf("getIdOrUUID = %q, want something different from %q", newgot, got)
	}
}

