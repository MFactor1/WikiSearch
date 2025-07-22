package main

import(
	"os"
	"bufio"
	"encoding/json"
	"log"

	"wxindexer/containers"
)

func jsonWriter(tfChan <- chan containers.PageTF) {
	err := os.MkdirAll("./localdata", 0755)
	if err != nil {
		panic(err)
	}
	f, err := os.Create("/run/media/matthewnesbitt/Linux 1TB SSD/WikiDump/.tf_output.jsonl")
	if err != nil {
		panic(err)
	}

	writer := bufio.NewWriter(f)
	defer f.Close()

	for {
		if page, ok := <- tfChan; ok {
			m_page, _ := json.Marshal(page)
			writer.Write(m_page)
			writer.Write([]byte("\n"))
		} else {
			log.Println("wxindexer/writer: exiting")
			break
		}
	}
	writer.Flush()
	writer_group.Done()
}
