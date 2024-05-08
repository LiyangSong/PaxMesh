## PAXOS Based Distributed Cluster

This project implemented PAXOS algorithm as described in Lamport's paper [*PAXOS Made Simple*](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf) in a distributed server cluster. PAXOS was applied to add fault tolerance and achieve consensus of updates amongst replicated state machine KV-store servers. 

Remote Procedure call (RPC) protocol was adopted for the convenience of cross-server communications.

To test the fault-tolerance ability of PAXOS, acceptors were configured to "fail" randomly and periodically.
