package main

import (
	"fmt"

	"guegan.org/alan/go-alan-es/morestrings"
	"github.com/google/go-cmp/cmp"
	"github.com/olivere/elastic/v7"

)

func main() {
	fmt.Println(morestrings.ReverseRunes("!oG ,olleH"))
	fmt.Println(cmp.Diff("Hello World", "Hello Go"))

}

