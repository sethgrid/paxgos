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

type prepareResponse struct {
	HighestProposalID  int
	HighestProposalVal int
	HighestAcceptedID  int
}

type acceptResponse struct {
	accepted    bool
	acceptedID  int
	acceptedVal int
}

type request struct {
	proposalID        int
	proposalVal       int
	highestAcceptedID int
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

	// these are leader specific communication channels and can be shared
	// in node creation (lets the nodes talk to the leader)
	inputChan := make(chan int)
	acceptResponseChan := make(chan acceptResponse)
	prepareResponseChan := make(chan prepareResponse)

	allNodes := make([]*node, 0)
	for i := 0; i <= numNodes; i++ {
		// these channels are node specific so a leader or learner can communicate
		// directly to a given node
		prepareRequestChan := make(chan request)
		acceptRequestChan := make(chan request)
		learnReceiveChan := make(chan acceptResponse)
		allNodes = append(allNodes, &node{
			id:                  i,
			commands:            make(map[int]int),
			acceptResponseChan:  acceptResponseChan,
			acceptRequestChan:   acceptRequestChan,
			prepareResponseChan: prepareResponseChan,
			prepareRequestChan:  prepareRequestChan,
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

	distinguishedLearners := []*node{allNodes[leader], allNodes[learnerA], allNodes[learnerB]}

	vlog("initial setup:\n\tNode Count: %d\n\tLeaderID: %d\n\tDistinguished Learner IDs: %d, %d", numNodes, leader, learnerA, learnerB)

	for i, _ := range allNodes {
		if i == leader {
			go runLeader(allNodes, leader, learnerA, learnerB, inputChan)
			continue
		}
		go runNode(allNodes[i], distinguishedLearners, allNodes)
	}

	// block for a while
	<-time.After(1 * time.Hour)
}

func runLeader(allNodes []*node, leader int, learnerA int, learnerB int, inputChan chan int) {
	go getInputDataForLeader(inputChan)

	maxStepsAhead := 2
	highestAcceptedID := -1
	proposalCounts := make(map[int]int)
	acceptCounts := make(map[int]int)
	history := make([]request, 0)

	var proposalID int
	// sending
	go func() {
		for {
			select {
			case in := <-inputChan:
				vlog("leader received input value %d", in)
				proposalID++

				// track the number of responses we get here
				proposalCounts[proposalID] = 0
				acceptCounts[proposalID] = 0

				// don't get too far ahead
				for proposalID > highestAcceptedID+maxStepsAhead {
					vlog("blocking proposal id %d > %d + %d", proposalID, highestAcceptedID, maxStepsAhead)
					<-time.After(1 * time.Second)
				}

				for _, n := range allNodes {
					go func(n *node, pID int, pVal int, highestAcceptedID int) {
						vlog("leader issuing proposal %d (%d) to node id %d", pID, pVal, n.id)
						n.prepareRequestChan <- request{
							proposalID:        pID,
							proposalVal:       pVal,
							highestAcceptedID: highestAcceptedID,
						}
					}(n, proposalID, in, highestAcceptedID)
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case pr := <-allNodes[leader].prepareResponseChan:
				vlog("leader received prepare response %#v", pr)
				proposalCounts[pr.HighestProposalID] = proposalCounts[pr.HighestProposalID] + 1
				if proposalCounts[pr.HighestProposalID] >= ((len(allNodes) / 2) + 1) {
					for _, n := range allNodes {
						go func(n *node, pID int, pVal int, highestAcceptedID int) {
							vlog("leader issuing accept request %d (%d) to node id %d", pID, pVal, n.id)
							n.acceptRequestChan <- request{
								proposalID:        pID,
								proposalVal:       pVal,
								highestAcceptedID: highestAcceptedID,
							}
						}(n, pr.HighestProposalID, pr.HighestProposalVal, highestAcceptedID)
					}
				}

			case ar := <-allNodes[leader].acceptResponseChan:
				vlog("leader received accept response %#v", ar)
				acceptCounts[ar.acceptedID] = acceptCounts[ar.acceptedID] + 1
				if acceptCounts[ar.acceptedID] >= ((len(allNodes) / 2) + 1) {
					// get more accepted than nodes/2 + 1 and we have a winner
					highestAcceptedID = ar.acceptedID
					history = append(history, request{proposalID: ar.acceptedID, proposalVal: ar.acceptedVal})
					vlog("leader sending accept response to distinguished learners: %#v", ar)

					allNodes[learnerA].learnReceiveChan <- ar
					allNodes[learnerB].learnReceiveChan <- ar
				}
			}
		}
	}()

	// block a while
	<-time.After(1 * time.Hour)
}

func runNode(n *node, learners []*node, allNodes []*node) {
	vlog("node id %d starting", n.id)

	promisedID := -1
	acceptedID := -1

	for {
		select {
		case r := <-n.acceptRequestChan:
			vlog("node id %d accept request %#v", n.id, r)
			if r.proposalID > acceptedID && r.proposalID >= promisedID {
				acceptedID = r.proposalID
				n.acceptResponseChan <- acceptResponse{accepted: true, acceptedID: acceptedID, acceptedVal: r.proposalVal}
			}
		case r := <-n.prepareRequestChan:
			vlog("node id %d prepare request %#v", n.id, r)
			if r.proposalID > promisedID {
				promisedID = r.proposalID
				n.prepareResponseChan <- prepareResponse{HighestAcceptedID: acceptedID, HighestProposalID: promisedID, HighestProposalVal: r.proposalVal}
			}
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
	prepareResponseChan chan prepareResponse
	prepareRequestChan  chan request
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
		inputChan <- (i * i)
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
