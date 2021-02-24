package main

import (
	"fmt"
	"os/exec"
)

func dupls(a *EsAdapter, dryRun bool) {
	dupls := checkDuplicatesBySize(a.signatureDuplicates())
	for _, duplArray := range *dupls {
		if dryRun {
			fmt.Print("open ")
			for _, v := range duplArray {
				fmt.Print(" ", v.Name)
			}
			fmt.Println()
		} else {
			for _, v := range duplArray[1:] {
				cmd := exec.Command("rm", v.Name)
				err := cmd.Run()
				if err != nil {
					panic(err)
				}
				err = a.deleteById(v.getIdOrUUID(),v.Name)
				if err != nil {
					panic(err)
				}
			}
		}
	}
}

func checkDuplicatesBySize(duplicates *[][]Signature) *[][]Signature {
	identifiedDuplicates := [][]Signature{}
	for _, v := range *duplicates {
		grouppedBySize := groupBySize(v)
		for _, g := range *grouppedBySize {
			if len(g) >= 2 {
				identifiedDuplicates = append(identifiedDuplicates, g)
			}
		}
	}
	return &identifiedDuplicates
}

func groupBySize(signatures []Signature) *[][]Signature {
	sizes := make(map[int64]int)
	for _, v := range signatures {
		sizes[v.Size] = len(sizes) // it will assign an ordinal index
	}
	var l = len(sizes)
	ret := make([][]Signature, l)
	if l > 1 {
		for _, v := range signatures {
			ret[sizes[v.Size]] = append(ret[sizes[v.Size]], v)
		}
	} else {
		ret[0] = signatures
	}
	return &ret
}