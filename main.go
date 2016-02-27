package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
)

type node struct{}

var Verbose bool

func main() {
	var numNodes int
	var startingPort int

	flag.BoolVar(&Verbose, "v", false, "set flag -v to see verbose logging")
	flag.IntVar(&numNodes, "nodes", 5, "set the number of nodes. Minimum 3.")
	flag.IntVar(&startingPort, "starting-port", 9000, "starting with this port, listen on port +1, +2, +3, ..., + number of nodes. A value of 0 will assign random ports.")
	flag.Parse()

	if numNodes < 3 {
		log.Printf("Got %d nodes, upping to minimum count of 3", numNodes)
		numNodes = 3
	}

	log.Printf("Starting %d nodes", numNodes)

	// init states
	acceptors.state = make(map[int]Payload)
	learners.history = make(map[int][]Payload)

	if err := runNodes(numNodes, startingPort); err != nil {
		log.Fatal(err)
	}
}

// index 0 will be the leader
var ports []string

func runNodes(count, startingPort int) error {
	ports = make([]string, 0)
	for i := 0; i <= count; i++ {
		port := ":0"
		if startingPort != 0 {
			port = fmt.Sprintf(":%d", startingPort+i)
		}
		l, err := net.Listen("tcp", port)
		if err != nil {
			log.Fatal(err)
		}

		ports = append(ports, strings.Trim(l.Addr().String(), "[:]"))

		r := mux.NewRouter()

		// only one proposer, the leader
		if i == 0 {
			r.HandleFunc("/propose/{val}", proposeHandler)
		}

		// acceptor endpoints
		r.HandleFunc("/prepare/{id}/{val}", prepareHandler)
		r.HandleFunc("/accept/{id}/{val}", acceptHandler)

		// learner endpoint
		r.HandleFunc("/learn/{id}/{val}", learnHandler)

		// default
		r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			nodeID := determineNodeID(r.Host)
			learners.Lock()
			defer learners.Unlock()
			content := "History:\n"
			for _, command := range learners.history[nodeID] {
				content += fmt.Sprintf("id %d val %d\n", command.AcceptedID, command.AcceptedVal)
			}
			w.Write([]byte(content))
		})

		log.Printf("Starting node on :%s", l.Addr().String())
		go func(r *mux.Router) {
			if err := http.Serve(l, r); err != nil {
				log.Print(err)
			}
		}(r)
	}

	// block
	wg := &sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()

	return nil
}

type Payload struct {
	NodeID int
	// phase 1.b acceptor response: I wont promise to anything below this
	// phase 2.b unused
	PromiseID int64
	// phase 1.b acceptor response: this is the highest accepted id I've accepted
	// phase 2.b acceptor response: this is the id I just accepted
	AcceptedID int64

	// learn phase
	AcceptedVal int
}

var prepareID int64

func proposeHandler(w http.ResponseWriter, r *http.Request) {
	prepareChan := make(chan Payload)
	acceptChan := make(chan Payload)

	// Phase 1.a
	val := mux.Vars(r)["val"]
	log.Printf("New proposed value: %v", val)
	id := atomic.AddInt64(&prepareID, 1)

	for i, _ := range ports {
		if i == 0 {
			// don't call the leader. we already are the leader
			continue
		}
		go func(i int) {
			resp, err := http.Get(fmt.Sprintf("http://localhost:%s/prepare/%d/%s", ports[i], id, val))
			if err != nil {
				log.Printf("error: phase 1.a - leader GET error with prepare id %d :: %v", id, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusNoContent {
				// node rejected our request
				return
			}

			data, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Printf("error: phase 1.a - leader Read error with prepare id %d :: %v", id, err)
				return
			}
			payload := Payload{}
			err = json.Unmarshal(data, &payload)
			if err != nil {
				log.Printf("error: phase 1.a - leader Unmarshal error with prepare id %d :: %v", id, err)
				return
			}
			select {
			case prepareChan <- payload:
			case <-time.After(1 * time.Minute):
				// don't leak goroutines
				return
			}
		}(i)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		counter := 0
		for {
			select {
			case p := <-prepareChan:
				log.Printf("got prepare response: %#v", p)

				// unusual edge case perhaps, but called for in the paxos algorythm
				if p.PromiseID > id {
					// racy. consider atomic all over the place? yuck.
					id = p.PromiseID
					// also adjust the prepareID for future requests
					swapped := atomic.CompareAndSwapInt64(&prepareID, atomic.LoadInt64(&prepareID), p.PromiseID)
					if !swapped {
						log.Println("WARNING: unable to swap for larger promise id")
					}
				}
				counter++
				if counter > len(ports)/2+1 {
					wg.Done()
				}
			case <-time.After(1 * time.Minute):
				// don't leak the goroutine
				return
			}
		}
	}()

	wg.Wait()
	// Phase 2.a; we got a majority of responses

	for i, _ := range ports {
		if i == 0 {
			// don't call the leader. we already are the leader
			continue
		}
		go func(i int) {
			resp, err := http.Get(fmt.Sprintf("http://localhost:%s/accept/%d/%s", ports[i], id, val))
			if err != nil {
				log.Printf("error: phase 2.a - leader GET error with accept id %d :: %v", id, err)
				return
			}
			defer resp.Body.Close()
			data, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Printf("error: phase 2.a - leader Read error with accept id %d :: %v", id, err)
				return
			}
			payload := Payload{}
			err = json.Unmarshal(data, &payload)
			if err != nil {
				log.Printf("error: phase 2.a - leader Unmarshal error with accept id %d :: %v", id, err)
				return
			}
			select {
			case acceptChan <- payload:
			case <-time.After(1 * time.Minute):
				// don't leak goroutines
				return
			}
		}(i)
	}

	wg2 := &sync.WaitGroup{}
	wg2.Add(1)

	go func() {
		counter := 0
		for {
			select {
			case p := <-acceptChan:
				log.Printf("got accept response: %#v", p)
				counter++
				if counter > len(ports)/2+1 {
					wg2.Done()
				}
			case <-time.After(1 * time.Minute):
				// don't leak the goroutine
				return
			}
		}
	}()

	wg2.Wait()
	w.Write([]byte(fmt.Sprintf("CommandID: %d, Value %v\n", id, val)))

	// communicate out to all learners

	for i, _ := range ports {
		go func(i int) {
			resp, err := http.Get(fmt.Sprintf("http://localhost:%s/learn/%d/%s", ports[i], id, val))
			if err != nil {
				log.Printf("error: phase learn - leader GET error with accept id %d :: %v", id, err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				log.Printf("error: phase learn - unable to get 200ok - id %d :: %v", id, err)
				return
			}
		}(i)
	}
}

// this global state unfortuneately syncs our nodes; will need something that does not
var acceptors struct {
	sync.Mutex
	// [id] Payload where Payload is the last response sent
	state map[int]Payload
}

func prepareHandler(w http.ResponseWriter, r *http.Request) {
	// Phase 1.b
	nodeID := determineNodeID(r.Host)
	vars := mux.Vars(r)
	id, err := strconv.ParseInt(vars["id"], 10, 64)
	if err != nil {
		log.Println("Node %d: Error in prepare, bad id - %v", nodeID, err)
	}
	val, err := strconv.Atoi(vars["val"])
	if err != nil {
		log.Println("Node %d: Error in prepare, bad val - %v", nodeID, err)
	}
	log.Printf("Node %d: New Prepare [id] value: [%d] %d", nodeID, id, val)

	acceptors.Lock()
	defer acceptors.Unlock()
	state, ok := acceptors.state[nodeID]
	if !ok {
		state = Payload{}
		state.NodeID = nodeID
	}

	if id < state.PromiseID || id < state.AcceptedID {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	state.PromiseID = id
	acceptors.state[nodeID] = state
	err = json.NewEncoder(w).Encode(state)
	if err != nil {
		log.Printf("Node %d: error encoding state: %v", err)
	}
}

func acceptHandler(w http.ResponseWriter, r *http.Request) {
	// Phase 2.b
	nodeID := determineNodeID(r.Host)
	vars := mux.Vars(r)
	id, err := strconv.ParseInt(vars["id"], 10, 64)
	if err != nil {
		log.Println("Node %d: Error in accept, bad id - %v", nodeID, err)
	}
	val, err := strconv.Atoi(vars["val"])
	if err != nil {
		log.Println("Node %d: Error in accept, bad val - %v", nodeID, err)
	}
	log.Printf("Node %d: New accept [id] value: [%d] %d", nodeID, id, val)

	acceptors.Lock()
	defer acceptors.Unlock()
	state, ok := acceptors.state[nodeID]
	if !ok {
		state = Payload{}
		state.NodeID = nodeID
	}

	if id < state.PromiseID || id < state.AcceptedID {
		w.WriteHeader(http.StatusNoContent)
		log.Printf("Node %d: command id %d less than promise id %d or accepted id %d", nodeID, id, state.PromiseID, state.AcceptedID)
		return
	}

	// if we use distinguished learners (more than the one leader), we would communicate to them here upon acceptance

	state.AcceptedID = id
	acceptors.state[nodeID] = state
	err = json.NewEncoder(w).Encode(state)
	if err != nil {
		log.Printf("Node %d: error encoding state: %v", err)
	}
}

var learners struct {
	sync.Mutex
	history map[int][]Payload
}

func learnHandler(w http.ResponseWriter, r *http.Request) {
	nodeID := determineNodeID(r.Host)
	vars := mux.Vars(r)
	id, err := strconv.ParseInt(vars["id"], 10, 64)
	if err != nil {
		log.Println("Node %d: Error in prepare, bad id - %v", nodeID, err)
	}
	val, err := strconv.Atoi(vars["val"])
	if err != nil {
		log.Println("Node %d: Error in prepare, bad val - %v", nodeID, err)
	}

	log.Printf("Node %d: learning id %d value %d", nodeID, id, val)

	learners.Lock()
	defer learners.Unlock()
	learners.history[nodeID] = append(learners.history[nodeID], Payload{NodeID: nodeID, AcceptedID: id, AcceptedVal: val})
}

// node id is based on location in the ports array due to how we started each node
func determineNodeID(requestHost string) int {
	parts := strings.Split(requestHost, ":")
	if len(parts) != 2 {
		return -1
	}
	port := parts[1]
	for i, p := range ports {
		if p == port {
			return i
		}
	}
	return -1
}
