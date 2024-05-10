package server;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a proposal's unique identifier within the Paxos protocol,
 * consisting of a sequence number and a node ID to ensure uniqueness and proper ordering.
 */
public class ProposalNumber implements Comparable<ProposalNumber>, Serializable {
    private final int sequenceNumber;
    private final String nodeId;

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public String getNodeId() {
        return nodeId;
    }

    public ProposalNumber(int sequenceNumber, String nodeId) {
        this.sequenceNumber = sequenceNumber;
        this.nodeId = nodeId;
    }

    @Override
    public int compareTo(ProposalNumber other) {
        int result = Integer.compare(this.sequenceNumber, other.sequenceNumber);
        if (result == 0) {
            result = this.nodeId.compareTo(other.nodeId);
        }
        return result;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.sequenceNumber, this.nodeId);
    }

    @Override
    public String toString() {
        return sequenceNumber +  "." + nodeId;
    }
}
