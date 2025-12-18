package pagerank

import (
	"encoding/gob"
	"path/filepath"
	"os"
	"bufio"
	"log"
	"math"
	"slices"
	"strings"

	"wxindexer/containers"
)

type NodeID uint32

type PageLinks struct {
	Incoming []NodeID
	NumOutgoing uint32
	Redirect NodeID
}

type PageGraph map[NodeID]*PageLinks

const nullID NodeID = math.MaxUint32
const fln_pg_graph = "pageweb"
const fln_id_to_url = "idtourl"
const fln_pg_score = "scores"
const damping_factor float64 = 0.85

var pg_graph = make(PageGraph)
var pg_score []float64
var pg_score_copy []float64
var url_to_id = make(map[string]NodeID)
var id_to_url []string
var next_index NodeID = 0
var edges = 0

func AddPage(page containers.PageLinkData) {

	var redirect_id NodeID = nullID
	if page.Redirect != nil {
		if id, ok := url_to_id[*page.Redirect]; ok {
			redirect_id = id
		} else {
			redirect_id = createID(*page.Redirect)
			pg_graph[redirect_id] = &PageLinks{
				Incoming: make([]NodeID, 0),
				NumOutgoing: 0,
				Redirect: nullID,
			}
		}
	}

	if id, ok := url_to_id[page.URL]; ok {
		(*pg_graph[id]).Redirect = redirect_id
		(*pg_graph[id]).NumOutgoing = uint32(len(*page.Links))
	} else {
		var new_id = createID(page.URL)
		pg_graph[new_id] = &PageLinks{
			Incoming: make([]NodeID, 0),
			NumOutgoing: uint32(len(*page.Links)),
			Redirect: redirect_id,
		}
	}

	if page.Links == nil || redirect_id != nullID {
		return
	}

	for link := range *page.Links {
		edges++
		if id, ok := url_to_id[link]; ok {
			(*pg_graph[id]).Incoming = append((*pg_graph[id]).Incoming, url_to_id[page.URL])
		} else {
			var new_id = createID(link)
			pg_graph[new_id] = &PageLinks{
				Incoming: []NodeID{url_to_id[page.URL]},
				NumOutgoing: 0,
				Redirect: nullID,
			}
		}
	}
}

func createID(url string) NodeID {
	var new_id NodeID
	if id, ok := url_to_id[url]; ok {
		return id
	} else {
		var url_copy = strings.Clone(url)
		new_id = next_index
		url_to_id[url_copy] = new_id
		id_to_url = append(id_to_url, url_copy)
		next_index++
		return new_id
	}
}

func DumpStructures(path string) {
	log.Printf("wxindexer/pageweb: dumping page web structures to: %s", path)
	var pg_graph_path = filepath.Join(path, fln_pg_graph)
	var id_to_url_path = filepath.Join(path, fln_id_to_url)
	var pg_score_path = filepath.Join(path, fln_pg_score)
	var err error

	err = writeStructure(pg_graph_path, &pg_graph)
	if err != nil {
		log.Printf("wxindexer/pageweb: Failed to dump pg_graph: %s", err)
	}

	err = writeStructure(id_to_url_path, &id_to_url)
	if err != nil {
		log.Printf("wxindexer/pageweb: Failed to dump id_to_url: %s", err)
	}

	err = writeStructure(pg_score_path, &pg_score)
	if err != nil {
		log.Printf("wxindexer/pageweb: Failed to dump pg_score: %s", err)
	}

	log.Printf("wxindexer/pageweb: Dumped structures for web of %d nodes", len(pg_graph))
}

func writeStructure(path string, structure any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}

	writer := bufio.NewWriter(f)
	defer f.Close()
	defer writer.Flush()

	var encoder = gob.NewEncoder(writer)
	encoder.Encode(structure)
	return nil
}

func LoadStructures(path string) {
	log.Printf("wxindexer/pageweb: loading page web structures from: %s", path)
	var pg_graph_path = filepath.Join(path, fln_pg_graph)
	var id_to_url_path = filepath.Join(path, fln_id_to_url)
	var pg_score_path = filepath.Join(path, fln_pg_score)
	var err error

	err = readStructure(pg_graph_path, &pg_graph)
	if err != nil {
		pg_graph = make(PageGraph)
		log.Printf("wxindexer/pageweb: Failed to load pg_graph: %s", err)
	}
	log.Printf("wxindexer/pageweb: Loaded pg_graph of size: %d", len(pg_graph))

	err = readStructure(id_to_url_path, &id_to_url)
	if err != nil {
		id_to_url = make([]string, 0)
		log.Printf("wxindexer/pageweb: Failed to load id_to_url: %s", err)
	}
	log.Printf("wxindexer/pageweb: Loaded id_to_url of size: %d", len(id_to_url))

	err = readStructure(pg_score_path, &pg_score)
	if err != nil {
		pg_score = make([]float64, 0)
		log.Printf("wxindexer/pageweb: Failed to load pg_score: %s", err)
	}
	log.Printf("wxindexer/pageweb: Loaded pg_scores of size: %d", len(pg_score))

	log.Printf("wxindexer/pageweb: Rebuilding url_to_id from id_to_url")
	next_index = NodeID(len(id_to_url))
	url_to_id = make(map[string]NodeID, len(id_to_url))
	for id, url := range id_to_url {
		url_to_id[url] = NodeID(id)
	}
}

func readStructure(path string, structure any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}

	reader := bufio.NewReader(f)
	defer f.Close()

	var decoder = gob.NewDecoder(reader)
	decoder.Decode(structure)
	return nil
}

func runIteration() {
	var broken_incoming int = 0
	for id_int := range pg_score {
		var sum_incoming float64 = 0
		var id = NodeID(id_int)
		if _, ok := pg_graph[id]; !ok || pg_graph[id] == nil {
			continue
		}
		for _, back_link := range (*pg_graph[id]).Incoming {
			if _, ok := pg_graph[NodeID(back_link)]; !ok || pg_graph[NodeID(back_link)] == nil {
				//log.Printf("ERROR: Incoming link %s does not exist!", id_to_url[back_link])
				broken_incoming++
				continue
			}
			/*
			existing_score := pg_score_copy[back_link]
			incoming_node := pg_graph[NodeID(back_link)]
			num_outgoing := incoming_node.NumOutgoing
			sum_incoming = sum_incoming + existing_score / float64(num_outgoing)
			*/
			sum_incoming = sum_incoming + pg_score_copy[back_link] / float64(pg_graph[NodeID(back_link)].NumOutgoing)
		}
		pg_score[id] = (1 - damping_factor) / float64(len(pg_graph)) + damping_factor * sum_incoming
	}
	log.Printf("Num broken incoming: %d", broken_incoming)
}

func RunPageRank(iterations int) map[string]float64 {
	for i := range iterations {
		log.Printf("wxindexer/pageweb: running PageRank iteration %d", i)
		copyPageScore()
		runIteration()
		//PprintScores()
		log.Printf("wxindexer/pageweb: Total rank delta over iteration %d: %v", i, getDelta())
	}
	DumpStructures("./localdata/pagegraph/")
	return exportPgScores()
}

func PreProcess() {
	log.Printf("wxindexer/pageweb: running preprocessing steps")
	resolveRedirects()
	dedupBacklinks()
	buildSecondaryStructures()
}

func exportPgScores() map[string]float64 {
	var scores = make(map[string]float64, len(pg_score))
	for id, score := range pg_score {
		if _, ok := pg_graph[NodeID(id)]; ok {
			scores[id_to_url[id]] = score
		}
	}
	return scores
}

func buildSecondaryStructures() {
	log.Printf("wxindexer/pageweb: building secondary structures")
	var starting_score float64 = 1 / float64(len(pg_graph))
	pg_score = slices.Repeat([]float64{starting_score}, len(id_to_url))
}

func resolveRedirects() {
	// We don't need to perform multiple iterations, as Wikipedia does not support
	// double-redirects (they can exist, but will not work as intended), so we
	// will not support them either.
	log.Printf("wxindexer/pageweb: resolving redirects")
	for id, data_ptr := range pg_graph {

		var data PageLinks
		if data_ptr != nil {
			data = *data_ptr
		} else {
			log.Printf("wxindexer/pageweb: ERROR: Page %d does not have a data entry, this is wrong.", id)
			continue
		}

		if data.Redirect != nullID {
			if _, ok := pg_graph[data.Redirect]; !ok || pg_graph[data.Redirect] == nil {
				log.Printf("wxindexer/pageweb: ERROR: Redirect page '%s' points to '%s', which does not exist", id_to_url[id], id_to_url[data.Redirect])
				continue
			} else {
				(*pg_graph[data.Redirect]).Incoming = slices.Concat((*pg_graph[data.Redirect]).Incoming, (*pg_graph[id]).Incoming)
			}
			delete(pg_graph, id)
		}
	}
}

func dedupBacklinks() {
	for id, data_ptr := range pg_graph {
		var temp_set = containers.SetFromSlice((*data_ptr).Incoming)
		temp_set.Remove(id)
		(*pg_graph[id]).Incoming = temp_set.ToSlice()
	}
}

func copyPageScore() {
	pg_score_copy = make([]float64, len(pg_score))
	copy(pg_score_copy, pg_score)
}

func getDelta() float64 {
	var delta float64 = 0
	for item, score := range pg_score {
		delta = delta + math.Abs(score - pg_score_copy[item])
	}
	return delta
}

func LogStats() {
	log.Printf("wxindexer/pageweb: pgmap has %d items and %d edges", len(pg_graph), edges)
}

func Pprint() {
	for key, data := range pg_graph {
		var pp_links = make([]string, 0, len((*data).Incoming))
		for _, id := range (*data).Incoming {
			pp_links = append(pp_links, id_to_url[id])
		}
		log.Printf("%s <- %v", id_to_url[key], pp_links)
	}
}

func PprintScores() {
	log.Printf("scores num: %d, copy: %d", len(pg_score), len(pg_score_copy))
	for pg, score := range pg_score {
		log.Printf("%s <- %v", id_to_url[pg], score)
	}
}

func GetScores() map[string]float64 {
	return exportPgScores()
}
