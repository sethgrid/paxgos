package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	var numNodes int
	var startingPort int

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

// index 0 will be the leader, all other node's id's will match index position
var ports []string

// runNodes creates http servers for a given number of nodes and blocks.
// A special endpoint is created for the leader node (node id 0), '/propose/{val}'.
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
		r.HandleFunc("/prepare/{id}/{val}", randomDelay(prepareHandler))
		r.HandleFunc("/accept/{id}/{val}", randomDelay(acceptHandler))

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

// introduce random delays in endpoints with occasional failure (ie, very long delay)
func randomDelay(fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if rand.Intn(20) == 1 {
			log.Println("long delay initiated...")
			<-time.After(time.Duration(1 * time.Minute))
		}
		<-time.After(time.Duration(rand.Intn(3000)+1) * time.Millisecond)
		fn.ServeHTTP(w, r)
	}
}

// Payload handles the couple of payloads that will be acceptor responses,
// but doubles as the struct that stores history on learning nodes
// (something something single responsiblity something something violation).
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

// prepareID is a counter for the distinguished proposer (leader). It is global to act stateful.
var prepareID int64
var lastAckedID int64

// proposeHandler allows the proposer to take in requests and
// will send a prepare and accept request to acceptors and
// will notify learners of new values. Pretty much Paxos lite.
func proposeHandler(w http.ResponseWriter, r *http.Request) {
	// for collecting responses from acceptors
	prepareChan := make(chan Payload)
	acceptChan := make(chan Payload)

	val, err := strconv.ParseInt(mux.Vars(r)["val"], 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("provided value should be an integer"))
		return
	}

	log.Printf("New proposed value: %d", val)
	id := atomic.AddInt64(&prepareID, 1)

	// don't get ahead of ourselves by too far...
	var maxGap int64 = 1
	for id > atomic.LoadInt64(&lastAckedID)+maxGap {
		<-time.After(1 * time.Second)
	}

	// Phase 1.a, blocks until we have a majority response
	phaseRunnerA("phase 1.a", id, val, prepareChan)

	// Phase 2.a; blocks until we have a majority reponse
	phaseRunnerA("phase 2.a", id, val, acceptChan)

	// learning phase
	atomic.SwapInt64(&lastAckedID, id)
	sendToLeaners(id, val)

	w.Write([]byte(fmt.Sprintf("CommandID: %d, Value %v\n", id, val)))
}

// this global state syncs our learner nodes; I don't think it is against the spirit to have this
var learners struct {
	sync.Mutex
	history map[int][]Payload
}

// learnHandler takes in a request and stores a command id and value to history
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

// this global state unfortuneately syncs our nodes; will need something that does not
var acceptors struct {
	sync.Mutex
	// [id] Payload where Payload is the last response sent
	state map[int]Payload
}

// prepareHandler acts as Phase 1.b. of Paxos, allowing an acceptor to take a prepare request
func prepareHandler(w http.ResponseWriter, r *http.Request) {
	nodeID := determineNodeID(r.Host)
	vars := mux.Vars(r)
	id, err := strconv.ParseInt(vars["id"], 10, 64)
	if err != nil {
		log.Println("Node %d: Error in prepare, bad id - %v", nodeID, err)
	}
	val, err := strconv.ParseInt(vars["val"], 10, 64)
	if err != nil {
		log.Println("Node %d: Error in prepare, bad val - %v", nodeID, err)
	}
	log.Printf("Node %d: New Prepare [id] value: [%d] %d", nodeID, id, val)

	state, err := phaseRunnerB("phase 2.b", nodeID, id, val)
	if err != nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// let the leader know our response
	err = json.NewEncoder(w).Encode(state)
	if err != nil {
		log.Printf("Node %d: error encoding state: %v", err)
	}
}

// aceptHandler acts as Phase 2.b of Paxos, allowing an acceptor to take an accept request
func acceptHandler(w http.ResponseWriter, r *http.Request) {
	nodeID := determineNodeID(r.Host)
	vars := mux.Vars(r)
	id, err := strconv.ParseInt(vars["id"], 10, 64)
	if err != nil {
		log.Println("Node %d: Error in accept, bad id - %v", nodeID, err)
	}
	val, err := strconv.ParseInt(vars["val"], 10, 64)
	if err != nil {
		log.Println("Node %d: Error in accept, bad val - %v", nodeID, err)
	}
	log.Printf("Node %d: New accept [id] value: [%d] %d", nodeID, id, val)

	state, err := phaseRunnerB("phase 2.b", nodeID, id, val)
	if err != nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// let the leader know our response
	err = json.NewEncoder(w).Encode(state)
	if err != nil {
		log.Printf("Node %d: error encoding state: %v", err)
	}
}

// phaseRunnerA handles phase 1.a and 2.a of this paxos toy
func phaseRunnerA(phase string, id int64, val int64, payloadChan chan Payload) {
	// go over each known node (registered in ports)
	for i, _ := range ports {
		if i == 0 {
			// don't call the leader. we already are the leader
			continue
		}

		// send out all the prepare requests
		go func(i int) {
			resp, err := http.Get(fmt.Sprintf("http://localhost:%s/prepare/%d/%d", ports[i], id, val))
			if err != nil {
				log.Printf("error: %s - leader GET error with prepare id %d :: %v", phase, id, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusNoContent {
				// node rejected our request
				return
			}

			data, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Printf("error: %s - leader Read error with prepare id %d :: %v", phase, id, err)
				return
			}
			payload := Payload{}
			err = json.Unmarshal(data, &payload)
			if err != nil {
				log.Printf("error: %s - leader Unmarshal error with prepare id %d :: %v", phase, id, err)
				return
			}
			select {
			case payloadChan <- payload:
			case <-time.After(1 * time.Minute):
				// don't leak goroutines
				return
			}
		}(i)
	}

	// collect prepare respsonses until we have a majority
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				if !strings.Contains(r.(string), "WaitGroup") {
					fmt.Println("Recovered", r)
				}
			}
		}()
		counter := 0
		for {
			select {
			case p := <-payloadChan:
				log.Printf("got %s response: %#v", phase, p)

				// unusual edge case perhaps, but called for in the paxos algorythm
				// on the promise stage
				if p.PromiseID > id {
					// racy. consider atomic all over the place? yuck.
					id = p.PromiseID
					// also adjust the prepareID for future requests
					atomic.SwapInt64(&prepareID, p.PromiseID)
				}
				counter++
				majority := len(ports) / 2
				if len(ports)%2 == 1 {
					majority += 1
				}
				log.Printf("looking for majority of %d", majority)
				if counter > majority {
					log.Printf("majority found")
					wg.Done()
				}
			case <-time.After(1 * time.Minute):
				// don't leak the goroutine
				return
			}
		}
	}()

	wg.Wait()
}

// phaseRunnerB handles phase 1.b and 2.b of this paxos toy
func phaseRunnerB(phase string, nodeID int, id int64, val int64) (Payload, error) {
	// TODO - figure out a better way to not lock all acceptors
	acceptors.Lock()
	defer acceptors.Unlock()

	// init acceptor state if needed
	state, ok := acceptors.state[nodeID]
	if !ok {
		state = Payload{}
		state.NodeID = nodeID
	}

	// if we have newer request already handled, reject this request
	if id < state.PromiseID || id < state.AcceptedID {
		log.Printf("Node %d: command id %d less than promise id %d or accepted id %d", nodeID, id, state.PromiseID, state.AcceptedID)
		return state, fmt.Errorf("rejected")
	}

	// if we use distinguished learners (more than the one leader), we would communicate to them here upon acceptance
	if phase == "phase 2.b" {
		// pass
	}

	// update state
	state.AcceptedID = id
	acceptors.state[nodeID] = state

	return state, nil
}

// sendToLearners is the learn phase of this paxos toy
func sendToLeaners(id, val int64) {
	for i, _ := range ports {
		go func(i int) {
			resp, err := http.Get(fmt.Sprintf("http://localhost:%s/learn/%d/%d", ports[i], id, val))
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

// determineNodeID gives us a node id based on the port location in the ports array.
// This works due to the implementation of storing nodes and ports globally.
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
