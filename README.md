## Paxgos

Paxgos is a toy Paxos implementation. It is in no way complete should probably not even be used for insipration for anything intended to be complete.

### What does it do?

It starts up n nodes with a single, known leader. The leader has a propose endpoint where you can submit integer values to the cluster (there is an auto propose endpoint that will send a batch of propsals out for you automatically).

Individual nodes may "fail" (have a large timeout). This demonstrates that we can proceed with new values being submitted to the cluster withough full consensus as long as a majority of nodes respond.

### Endpoints

Leader Specific:
 - `/propose/{int}`
 - `/autopropose`

Internal Node Endpoints (the leader does all the work)
 - `/prepare/{id}/{val}`
 - `/accept/{id}/{val}`
 - `/learn/{id}/{val}`

All nodes expose their root `/` to see what commands have been committed to them.

### Relation to Paxos

Inspired from the [Paxos Made Simple](http://research.microsoft.com/en-us/um/people/lamport/pubs/paxos-simple.pdf) paper, the code provides nodes that fit the roles of proposer, acceptors, and learers. The leader node acts as a distinguished proposer and distinguished learner, and the other nodes acts as acceptors. The meat of the Paxos Algorithm is composed of two phases, and these phases are called out in the code:
```
Phase 1.
(a) A proposer selects a proposal number n and sends a prepare request with number n to a majority of acceptors.
(b) If an acceptor receives a prepare request with number n greater than that of any prepare request to which it has already responded, then it responds to the request with a promise not to accept any more proposals numbered less than n and with the highest-numbered proposal (if any) that it has accepted.

 Phase 2.
(a) If the proposer receives a response to its prepare requests (numbered n) from a majority of acceptors, then it sends an accept request to each of those acceptors for a proposal numbered n with a value v, where v is the value of the highest-numbered proposal among the responses, or is any value if the responses reported no proposals.
(b) If an acceptor receives an accept request for a proposal numbered n, it accepts the proposal unless it has already responded to a prepare request having a number greater than n
```

The code presented runs through these phases. It does not concern itself with leader election or having multiple distinguished learners for resiliency. It just explores these two phases.