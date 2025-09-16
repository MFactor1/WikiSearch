package pagerank

import (
	"encoding/gob"
	"path/filepath"
	"os"
	"bufio"
	"log"
	"sync"

	"wxindexer/containers"
)

type PageLinks struct {
	Incoming []string
	Redirect *string
}

type PageGraph map[string]PageLinks

var pg_graph = make(PageGraph)
var pg_list []string
var pg_visits = make(map[string]int)
var visit_lock sync.Mutex

func AddPage(page containers.PageLinkData) {
	if page_links, ok := pg_graph[page.URL]; !ok {
		pg_graph[page.URL] = PageLinks{Incoming: make([]string, 0), Redirect: page.Redirect}
	} else {
		page_links.Redirect = page.Redirect
	}

	if page.Links == nil {
		return
	}

	for _, link := range *page.Links {
		if page_links, ok := pg_graph[link]; ok {
			page_links.Incoming = append(page_links.Incoming, page.URL)
			pg_graph[link] = page_links
		} else {
			pg_graph[link] = PageLinks{Incoming: []string{page.URL}, Redirect: nil}
		}
	}
}

func DumpPageWeb(path string) {
	log.Printf("wxindexer/pageweb: dumping page web to: %s", path)
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

func LoadPageWeb(path string) {
	log.Printf("wxindexer/pageweb: loading page web from: %s", path)
	f, err := os.Open(path)
	if err != nil {
		log.Printf("wxindexer/pageweb: Failed to load existing PageGraph: %s", err)
		pg_graph = make(PageGraph)
		return
	}

	reader := bufio.NewReader(f)
	defer f.Close()

	var decoder = gob.NewDecoder(reader)
	decoder.Decode(&pg_graph)
	BuildSecondaryStructures()
}

func BuildSecondaryStructures() {
	log.Printf("wxindexer/pageweb: building secondary structures")
	for key, _ := range pg_graph {
		pg_list = append(pg_list, key)
		pg_visits[key] = 0
	}
}

func LogStats() {
	log.Printf("wxindexer/pageweb: pgmap size: %d", len(pg_graph))
}

func Pprint() {
	for key, links := range pg_graph {
		log.Printf("%s <- %v", key, links.Incoming)
	}
}
