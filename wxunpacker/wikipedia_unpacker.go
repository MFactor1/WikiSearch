package main

import (
	"encoding/xml"
	"log"
	"io"
	"os"
	"strings"
	"net"
	"net/url"
	"time"
	"regexp"
	"bufio"
	"github.com/vmihailenco/msgpack/v5"
	"common"
)

type Page struct {
	Title string `xml:"title"`
	Text string `xml:"revision>text"`
	Namespace string `xml:"ns"`
}

var reRedirect = regexp.MustCompile(`^#REDIRECT \[\[(.*?)\]\]`)

func main() {
	num_pages, err := countPagesCustomDecoder("/run/media/matthewnesbitt/Linux 1TB SSD/WikiDump/enwiki-20250320-pages-articles-multistream.xml")
	if err != nil {
		panic(err)
	}

	log.Println("Starting Indexing")
	addr := "/tmp/windexIPC.sock"
	connection, err := net.Dial("unix", addr)
	if err != nil {
		panic(err)
	}
	defer connection.Close()

	encoder := msgpack.NewEncoder(connection)

	file, err := os.Open("/run/media/matthewnesbitt/Linux 1TB SSD/WikiDump/enwiki-20250320-pages-articles-multistream.xml")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	//bz2_reader := bzip2.NewReader(file)
	decoder := xml.NewDecoder(file)

	var page Page
	var i = 0
	var diff = 0
	var send_chan = make(chan common.PageData, 1000)

	go sendPages(send_chan, encoder)

	start := time.Now()
	for {
		tok, err := decoder.Token()
		if err == io.EOF {
			elapsed := time.Since(start).Seconds()
			log.Printf("wxunpacker: Reached EOF, processed %d pages in %dh, %dm, %ds\n",
			i,
			int(elapsed) / 3600,
			(int(elapsed) % 3600) / 60,
			int(elapsed) % 60,
		)
			break
		} else if err != nil {
			panic(err)
		}

		switch element := tok.(type) {
		case xml.StartElement:
			if element.Name.Local != "page" {
				continue
			}
			page = Page{}
			decoder.DecodeElement(&page, &element)
			if page.Namespace != "0" || page.Title == "" {
				continue
			}
			if diff >= 1000 {
				elapsed := time.Since(start).Seconds()
				completion := float64(i) / float64(num_pages)
				etr_s := int(float64(elapsed) / completion - float64(elapsed))
				log.Printf("wxunpacker: Processed:%d/%d, %.2f%%, ETR: %dh, %dm, %ds, Elapsed: %dh, %dm, %ds\n",
					i,
					num_pages,
					100 * completion,
					etr_s / 3600,
					(etr_s % 3600) / 60,
					etr_s % 60,
					int(elapsed) / 3600,
					(int(elapsed) % 3600) / 60,
					int(elapsed) % 60,
				)
				diff = 0
			}
			diff++
			i++

			url_title := url.PathEscape(strings.ReplaceAll(page.Title, " ", "_"))
			send_chan <- common.PageData{Title: page.Title, URL: url_title, Body: page.Text}
		default:
		}
	}
}

func countPagesWithXMLDecoder(path string) (int, error) {
	//return 8032054, nil
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	decoder := xml.NewDecoder(file)
	var page Page

	count := 0
	diff := 0
	for {
		tok, err := decoder.Token()
		if err == io.EOF {
			break
		} else if err != nil {
			return count, err
		}

		if se, ok := tok.(xml.StartElement); ok && se.Name.Local == "page" {
			decoder.DecodeElement(&page, &se)
			if page.Namespace == "0" && page.Title != "" {
				if diff >= 1000 {
					log.Println("wxunpacker: preprocessed:", count)
					diff = 0
				}
				diff++
				count++
			}
		}
	}

	return count, nil
}

func countPagesCustomDecoder(path string) (int, error) {
	return 18334374, nil
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 1024*1024)

	var in_page = false
	var ns_valid = false
	var title_valid = false
	var pages = 0
	var invalid = 0
	var count = 0
	var total = 0
	var done = false

	for !done {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			done = true
		} else if err != nil {
			log.Fatal(err)
		}
		line = strings.TrimSpace(line)
		if in_page {
			if ns, ok := strings.CutPrefix(line, "<ns>"); ok {
				ns := strings.TrimSuffix(ns, "</ns>")
				//log.Printf("Found ns %s\n", line)
				if ns == "0" {
					//log.Printf("ns valid")
					ns_valid = true
				} else {
					//log.Printf("ns INVALID: %s\n", line)
					in_page = false
					invalid++
				}
			} else if title, ok := strings.CutPrefix(line, "<title>"); ok {
				title := strings.TrimSuffix(title, "</title>")
				//log.Printf("Found title %s\n", line)
				if title != "" {
					//log.Printf("title valid")
					title_valid = true
				} else {
					//log.Printf("title INVALID: %s\n", line)
					in_page = false
					invalid++
				}
			} else if strings.HasPrefix(line, "</page>") {
				//log.Printf("found end of page")
				if ns_valid && title_valid {
					//log.Printf("Page valid")
					pages++
					count++
					if count >= 10000 {
						log.Printf("wxunpacker/preprocesser: preprocessed: %d\n", pages)
						log.Printf("wxunpacker/preprocesser: invalid: %d\n", invalid)
						log.Printf("wxunpacker/preprocesser: total: %d\n", total)
						count = 0
						//return pages, nil
					}
				} else {
					//log.Printf("Page rejected")
					invalid++
				}
				ns_valid = false
				title_valid = false
				in_page = false
			}
		} else {
			if strings.HasPrefix(line, "<page>") {
				//log.Printf("Found page: %s\n", line)
				in_page = true
				total++
			}
		}
	}
	return pages, nil
}

func sendPages(in_chan <- chan common.PageData, sock *msgpack.Encoder) {
	var diff = 0
	var wait int64 = 0
	for {
		page := <- in_chan
		start := time.Now()
		err := sock.Encode(page)
		wait = time.Since(start).Microseconds() + wait
		if err != nil {
			panic(err)
		}
		if diff >= 1000 {
			//log.Printf("wxunpacker: Avg send wait time: %d\n", wait / 1000)
			diff = 0
			wait = 0
		}
		diff++
	}
}
