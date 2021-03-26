package main

import "log"

type FileMover interface {
	MoveSecurely(esInterface EsInterface, v Signature, newpath string) error
	EndReport() error
}

type FileMoverImpl struct {
	nbCalls int
}

type FileMoverDryRunImpl struct {
	FileMoverImpl
}

func NewFileMover(dryRun bool) FileMover {
	if dryRun {
		return &FileMoverDryRunImpl{}
	}
	return &FileMoverImpl{}
}

func (a *FileMoverImpl) MoveSecurely(es EsInterface, v Signature, newpath string) error {
	a.nbCalls++
	err := MoveFile(v.Name, newpath)
	if err != nil {
		log.Print("Could not rename ", v.Name, " into ", newpath, " ", err)
		return err
	} else {
		err := es.SavePathChange(v, newpath)
		if err != nil {
			MoveFile(newpath, v.Name)
			log.Print("Rolled Back rename ", v.Name, " into ", newpath, " ", err)
			return err
		}
		log.Println(v.Name, " treated into ", newpath)
	}
	return nil
}

func (f *FileMoverImpl) EndReport() error {
	log.Print("Nb calls perforned to the file mover: ", f.nbCalls)
	return nil
}

func (f *FileMoverDryRunImpl) MoveSecurely(esInterface EsInterface, v Signature, newpath string) error {
	f.nbCalls++
	log.Print("Requested to rename ", v.Name, " into ", newpath, " but dry run mode")
	return nil
}


