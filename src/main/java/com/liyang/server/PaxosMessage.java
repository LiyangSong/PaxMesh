package com.liyang.server;


import com.liyang.paxosNode.PaxosNode;
import lombok.Getter;

import java.io.Serializable;
import java.util.UUID;


/**
 * Encapsulates a message used in the Paxos protocol,
 * carrying all necessary information such as proposal ID, type of message,
 * and operation details needed for decision-making in the consensus process.
 */
@Getter
public class PaxosMessage implements Serializable {
    private final UUID proposalId;
    private final PaxosMessageType messageType;
    private final ProposalNumber proposalNumber;
    private final PaxosNode fromNode;
    private final PaxosNode toNode;
    private final OperationType operationType;
    private final String key;
    private final String value;

    public PaxosMessage(UUID proposalId, PaxosMessageType messageType, ProposalNumber proposalNumber, PaxosNode fromNode, PaxosNode toNode, OperationType operationType, String key, String value) {
        this.proposalId = proposalId;
        this.messageType = messageType;
        this.proposalNumber = proposalNumber;
        this.fromNode = fromNode;
        this.toNode = toNode;
        this.operationType = operationType;
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return String.format(
                """
                ========================================
                PaxosMessage {
                proposalId: %s
                messageType: %s
                proposalNumber: %s
                fromNode: %s
                toNode: %s
                operationType: %s
                key: %s
                value: %s }
                ========================================
                """,
                proposalId, messageType, proposalNumber, fromNode, toNode, operationType, key, value);
    }
}
