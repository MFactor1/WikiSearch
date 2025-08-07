package main

import (
	"encoding/gob"
	"path/filepath"
	"os"
	"bufio"
	"log"

	"wxindexer/containers"
)

type PageLinks struct {
	Links *[]string
	Redirect *string
}

type PageGraph map[string]PageLinks

var pg_graph = make(PageGraph)

func addPage(page containers.PageLinkData) {
	if page.Redirect != nil {
		pg_graph[page.URL] = PageLinks{Links: nil, Redirect: page.Redirect}
	} else {
		pg_graph[page.URL] = PageLinks{Links: page.Links, Redirect: nil}
	}
}

func dumpPageWeb(path string) {
	log.Printf("wxindexer/pgmapper: dumping page web to: %s", path)
	err := os.MkdirAll(filepath.Dir(path), 0755)
	if err != nil {
		panic(err)
	}

	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}

	writer := bufio.NewWriter(f)
	defer f.Close()
	defer writer.Flush()

	var encoder = gob.NewEncoder(writer)
	encoder.Encode(pg_graph)
}

func loadPageWeb(path string) {
	log.Printf("wxindexer/pgmapper: loading page web from: %s", path)
	f, err := os.Open(path)
	if err != nil {
		log.Printf("wxindexer/pgmapper: Failed to load existing PageGraph: %s", err)
		pg_graph = make(PageGraph)
		return
	}

	reader := bufio.NewReader(f)
	defer f.Close()

	var decoder = gob.NewDecoder(reader)
	decoder.Decode(&pg_graph)
}

func logStats() {
	log.Printf("wxindexer/pgmapper: pgmap size: %d", len(pg_graph))
}
