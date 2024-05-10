package server;

import paxosInterface.PaxosNode;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Maintains the state and context of a single Paxos proposal,
 * tracking sequence numbers, committed nodes, and consensus status,
 * among other details necessary for the protocol's operations.
 */
public class ProposalContext implements Serializable {
    private final UUID proposalId; // Unique identifier for the proposal
    private int largestSequenceNumber = 0;
    private ProposalNumber largestProposalNumber = null;
    private final Map<PaxosNode, Boolean> committedNodes = new ConcurrentHashMap<>();
    private boolean commitConsensus = false;  // Commit consensus has achieved
    ProposalNumber promisedProposalNumber = null;  // the highest proposal number that has promised by Acceptor
    private final Map<PaxosNode, Boolean> acceptedNodes = new ConcurrentHashMap<>();
    private boolean acceptedConsensus = false;  // Learn consensus has achieved
    private Operation finalOperation = null;

    public ProposalContext(UUID proposalId) {
        this.proposalId = proposalId;
    }

    public UUID getProposalId() {
        return proposalId;
    }

    public int getLargestSequenceNumber() {
        return largestSequenceNumber;
    }

    public ProposalNumber getLargestProposalNumber() {
        return largestProposalNumber;
    }

    public Map<PaxosNode, Boolean> getCommittedNodes() {
        return committedNodes;
    }

    public ProposalNumber getPromisedProposalNumber() {
        return promisedProposalNumber;
    }

    public Map<PaxosNode, Boolean> getAcceptedNodes() {
        return acceptedNodes;
    }

    public Operation getFinalOperation() {
        return finalOperation;
    }

    public void setLargestSequenceNumber(int largestSequenceNumber) {
        this.largestSequenceNumber = largestSequenceNumber;
    }

    public void setLargestProposalNumber(ProposalNumber largestProposalNumber) {
        this.largestProposalNumber = largestProposalNumber;
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

    public void setPromisedProposalNumber(ProposalNumber promisedProposalNumber) {
        this.promisedProposalNumber = promisedProposalNumber;
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

    public boolean isCommitConsensus() {
        return commitConsensus;
    }

    public void setCommitConsensus(boolean commitConsensus) {
        this.commitConsensus = commitConsensus;
    }

    public boolean isAcceptedConsensus() {
        return acceptedConsensus;
    }

    public void setAcceptedConsensus(boolean acceptedConsensus) {
        this.acceptedConsensus = acceptedConsensus;
    }

    public void setFinalOperation(Operation finalOperation) {
        this.finalOperation = finalOperation;
    }
}
