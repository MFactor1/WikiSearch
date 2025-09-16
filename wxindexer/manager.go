package main

import (
	"log"
	"net"
	"os"
	"io"
	"time"
	"bufio"
	"sync"

	"common"
	"wxindexer/cleaners"
	"wxindexer/containers"
	"wxindexer/pagerank"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/redis/go-redis/v9"
)

var (
	workers = 12
	reader_group sync.WaitGroup
	indexer_group sync.WaitGroup
	writer_group sync.WaitGroup
)

func main() {
	log.Println("wxindexer/manager: initalizing cleaner")
	cleaner := cleaners.NewWikipediaCleaner()

	log.Println("wxindexer/manager: initializing redis client")
	rdb := newRedisClient()

	log.Println("wxindexer/manager: loading stopwords")
	stopwords, err := loadStopWords()
	if err != nil {
		panic(err)
	}

	addr := "/tmp/windexIPC.sock"
	os.Remove(addr)

	listener, err := net.Listen("unix", addr)
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	log.Println("wxindexer/manager: waiting for connection...")
	connection, err := listener.Accept()
	if err != nil {
		panic(err)
	}
	defer connection.Close()
	log.Println("wxindexer/manager: connection established")

	decoder := msgpack.NewDecoder(connection)

	index_chan := make(chan common.PageData, 1000)
	write_chan := make(chan containers.PageTF, 1000)
	pg_map_chan := make(chan containers.PageLinkData, 1000)

	go func() {
		for {
			log.Printf("wxindexer/manager: Indexing Queue: %d", len(index_chan))
			log.Printf("wxindexer/manager: Write Queue: %d", len(write_chan))
			log.Printf("wxindexer/manager: Page Map Queue: %d", len(pg_map_chan))
			pagerank.LogStats()
			time.Sleep(1 * time.Second)
		}
	}()

	reader_group.Add(1)
	writer_group.Add(2)
	indexer_group.Add(workers)

	go socketReader(decoder, index_chan)
	go jsonWriter(write_chan)
	go pgMapper(pg_map_chan)

	for i := range workers {
		go indexer(i, cleaner, stopwords, rdb, index_chan, write_chan, pg_map_chan)
	}

	reader_group.Wait()
	close(index_chan)
	indexer_group.Wait()
	close(write_chan)
	close(pg_map_chan)
	writer_group.Wait()
}

func newRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options {
		Addr: "localhost:6380",
	})
}

func loadStopWords() (*containers.Set, error) {
	file, err := os.Open("./data/stopwords")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	stopwords := containers.NewSet()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		stopwords.Add(line)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return stopwords, nil
}

func socketReader(decoder *msgpack.Decoder, out_chan chan <- common.PageData) {
	var diff = 0
	var wait int64 = 0

	for {
		var page common.PageData
		start := time.Now()
		err := decoder.Decode(&page)
		wait = time.Since(start).Microseconds() + wait
		if diff >= 1000 {
			//log.Println("wxindexer/reader: avg recieve wait time:", wait / 1000)
			diff = 0
			wait = 0
		}
		diff++
		if err != nil {
			if err == io.EOF {
				log.Println("wxindexer/reader: connection closed by sender. Exiting.")
				reader_group.Done()
				return
			}
			log.Printf("wxindexer/reader: decoder error: %v", err)
		}
		out_chan <- page
	}
}

func indexer(
	id int,
	cleaner cleaners.Cleaner,
	stopwords *containers.Set,
	rdb *redis.Client,
	in_chan <- chan common.PageData,
	write_chan chan <- containers.PageTF,
	pg_map_chan chan <- containers.PageLinkData) {

	var tf containers.PageTF
	for {
		if page, ok := <- in_chan; ok {
			tf = index(page, cleaner, stopwords, rdb)
			write_chan <- tf
			pg_map_chan <- containers.PageLinkData{URL: tf.URL, Links: &tf.Links, Redirect: tf.Redirect}
		} else {
			log.Printf("wxindexer/indexer@%d: exiting\n", id)
			break
		}
	}
	indexer_group.Done()
}

func pgMapper(pg_map_chan <- chan containers.PageLinkData) {
	pagerank.LoadPageWeb("./localdata/pagegraph/.pagegraph")
	for pg := range pg_map_chan {
		pagerank.AddPage(pg)
	}
	pagerank.DumpPageWeb("./localdata/pagegraph/.pagegraph")
	log.Println("wxindexer/pgmapper: exiting")
	writer_group.Done()
}
