package com.liyang.server;

import com.liyang.paxosNode.PaxosNode;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Maintains the state and context of a single Paxos proposal,
 * tracking sequence numbers, committed nodes, and consensus status,
 * among other details necessary for the protocol's operations.
 */
@Getter
public class ProposalContext implements Serializable {
    private final UUID proposalId; // Unique identifier for the proposal
    @Setter
    private int largestSequenceNumber = 0;
    @Setter
    private ProposalNumber largestProposalNumber = null;
    private final Map<PaxosNode, Boolean> committedNodes = new ConcurrentHashMap<>();
    @Setter
    private boolean commitConsensus = false;  // Commit consensus has achieved
    @Setter
    ProposalNumber promisedProposalNumber = null;  // the highest proposal number that has promised by Acceptor
    private final Map<PaxosNode, Boolean> acceptedNodes = new ConcurrentHashMap<>();
    @Setter
    private boolean acceptedConsensus = false;  // Learn consensus has achieved
    @Setter
    private Operation finalOperation = null;

    public ProposalContext(UUID proposalId) {
        this.proposalId = proposalId;
    }

    public void addCommittedNode(PaxosNode node) {
        committedNodes.put(node, Boolean.TRUE);
    }

    public void clearCommittedNodes() {
        committedNodes.clear();
    }

    public boolean achieveMajorityCommitted(int totalNum) {
        return committedNodes.size() > totalNum / 2;
    }

    public void addAcceptedNode(PaxosNode node) {
        acceptedNodes.put(node, Boolean.TRUE);
    }

    public void clearAcceptedNodes() {
        acceptedNodes.clear();
    }

    public boolean achieveMajorityAccepted(int totalNum) {
        return acceptedNodes.size() > totalNum / 2;
    }

}
