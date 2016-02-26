package main

import (
	"flag"
	"log"
	"math/rand"
	"time"
)

// need to simulate troubled network
// need to monitor all nodes

// make a pool of machines
// elect a leader (distinguished leader and learner)
// elect more distiguished learners for redundancy
// pass a message to the leader from outside
// send a proposal to acceptors
// acceptors ack with promise to not go lower
// send accept request to acceptors
// acceptors accept if number is greater than they've seen, notify distinguished learners

type promiseResponse struct {
	MinAcceptID       int
	HighestAcceptedID int
}

type acceptResponse struct {
	accepted    bool
	acceptedID  int
	acceptedVal int
}

type request struct {
	proposalID  int
	proposalVal int
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

var Verbose bool

func main() {
	var numNodes int

	flag.BoolVar(&Verbose, "v", false, "set flag -v to see verbose logging")
	flag.IntVar(&numNodes, "nodes", 5, "set the number of nodes. Minimum 3.")
	flag.Parse()

	if numNodes < 3 {
		numNodes = 3
	}

	inputChan := make(chan int)
	acceptResponseChan := make(chan acceptResponse)
	promiseResponseChan := make(chan promiseResponse)

	allNodes := make([]*node, 0)
	for i := 0; i <= numNodes; i++ {
		promiseRequestChan := make(chan request)
		acceptRequestChan := make(chan request)
		learnReceiveChan := make(chan acceptResponse)
		allNodes = append(allNodes, &node{
			id:                  i,
			commands:            make(map[int]int),
			acceptResponseChan:  acceptResponseChan,
			acceptRequestChan:   acceptRequestChan,
			promiseResponseChan: promiseResponseChan,
			promiseRequestChan:  promiseRequestChan,
			learnReceiveChan:    learnReceiveChan,
		})
	}

	// establish some initial distinguished roles
	leader := rand.Intn(numNodes)
	learnerA := leader + 1
	learnerB := leader + 2
	if learnerA >= numNodes {
		learnerA = 0
	}
	if learnerB >= numNodes {
		learnerB = 1
	}

	distinguishedLearners := []*node{allNodes[learnerA], allNodes[learnerB]}

	vlog("initial setup:\n\tNode Count: %d\n\tLeaderID: %d\n\tDistinguished Learner IDs: %d, %d", numNodes, leader, learnerA, learnerB)

	go runLeader(allNodes, leader, inputChan)

	for i, _ := range allNodes {
		if i == leader {
			continue
		}
		go runNode(allNodes[i], distinguishedLearners)
	}

	// block for a while
	<-time.After(1 * time.Hour)
}

func runLeader(allNodes []*node, leader int, inputChan chan int) {
	go getInputDataForLeader(inputChan)

	maxStepsAhead := 2
	currentStep := 0

	var proposalID int
	for {
		select {
		case in := <-inputChan:
			vlog("leader received input value %d", in)
			proposalID++

			// don't get too far ahead
			for proposalID > currentStep+maxStepsAhead {
				<-time.After(1 * time.Second)
			}

			for _, n := range allNodes {
				go func(n *node, pID int, pVal int) {
					vlog("leader issuing proposal %d (%d) to node id %d", pID, pVal, n.id)
					n.promiseRequestChan <- request{
						proposalID:  pID,
						proposalVal: pVal,
					}
				}(n, proposalID, in)
			}
		case pr := <-allNodes[leader].promiseResponseChan:
			vlog("leader received promise response %#v", pr)
		case ar := <-allNodes[leader].acceptResponseChan:
			vlog("leader received accept response %#v", ar)
		}
	}
}

func runNode(n *node, learners []*node) {
	vlog("node id %d starting", n.id)
	for {
		select {
		case r := <-n.acceptRequestChan:
			vlog("node id %d accept request %#v", n.id, r)
		case r := <-n.promiseRequestChan:
			vlog("node id %d promise request %#v", n.id, r)
		case r := <-n.learnReceiveChan:
			vlog("node id %d learning request %#v", n.id, r)
		}
	}
}

type node struct {
	id       int
	isLeader bool

	acceptResponseChan  chan acceptResponse
	acceptRequestChan   chan request
	promiseResponseChan chan promiseResponse
	promiseRequestChan  chan request
	learnReceiveChan    chan acceptResponse

	currentProposalID int
	currentValueID    int

	currentMinAcceptedID     int
	currentHighestAcceptedID int

	// id to value
	commands map[int]int
}

func getInputDataForLeader(inputChan chan int) {
	// read in data somewhere
	for i := 0; i <= 10; i++ {
		vlog("Sending input value %d to leader", i)
		inputChan <- i * i
	}
}

func vlog(format string, v ...interface{}) {
	if Verbose {
		log.Printf(format, v...)
	}
}

/*

Client   Proposer      Acceptor     Learner
   |         |          |  |  |       |  | --- First Request ---
   X-------->|          |  |  |       |  |  Request
   |         X--------->|->|->|       |  |  Prepare(N)
   |         |<---------X--X--X       |  |  Promise(N,I,{Va,Vb,Vc})
   |         X--------->|->|->|       |  |  Accept!(N,I,Vm)
   |         |<---------X--X--X------>|->|  Accepted(N,I,Vm)
   |<---------------------------------X--X  Response
   |         |          |  |  |       |  |

*/
