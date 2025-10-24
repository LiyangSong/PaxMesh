package com.liyang.server;

import com.liyang.paxosNode.PaxosNode;

import java.rmi.RemoteException;
import java.util.Map;
import java.util.UUID;


/**
 * Implements the Proposer role in Paxos, initiating proposals and handling responses from Acceptors.
 */
public class Proposer {
    private final PaxosNodeImpl node;
    private final Map<UUID, ProposalContext> contextStore;

    public Proposer(PaxosNodeImpl node, Map<UUID, ProposalContext> contextStore) {
        this.node = node;
        this.contextStore = contextStore;
    }

    /**
     * Starts a new proposal by generating a unique proposal ID
     * and a higher sequence number to ensure progress in the Paxos rounds.
     */
    public PaxosMessage initiateProposal(UUID proposalId, OperationType operationType, String key, String value) throws RemoteException {
        ProposalContext context = contextStore.getOrDefault(proposalId, new ProposalContext(proposalId));
        node.getOtherNodes().forEach(otherNode -> {
            try {
                Map<UUID, ProposalContext> otherContextStore = otherNode.getContextStore();
                ProposalContext otherContext = otherContextStore.getOrDefault(proposalId, new ProposalContext(proposalId));
                otherNode.updateContextStore(proposalId, otherContext);
            } catch (RemoteException e) {
                throw new RuntimeException(e);
            }
        });

        int sequenceNumber = context.getLargestSequenceNumber() + 1;
        ProposalNumber proposalNumber = new ProposalNumber(sequenceNumber, node.getNodeId());

        context.setLargestSequenceNumber(sequenceNumber);
        context.setLargestProposalNumber(proposalNumber);
        node.updateContextStore(proposalId, context);

        PaxosMessage proposal = new PaxosMessage(
                proposalId,
                PaxosMessageType.PREPARE,
                proposalNumber,
                node,
                null,
                operationType,
                key,
                value
        );
        ServerLogger.log(node.getNodeId(), "Proposer - Initiated a new proposal:\n" + proposal);

        return proposal;
    }

    /**
     * Sends PREPARE requests to all Acceptors to gather promises
     * and move forward in the Paxos consensus process.
     */
    public void sendPrepareRequests(PaxosMessage request) throws RemoteException {
        // Send to self acceptor
        sendRequestWithRetries(request, PaxosMessageType.PREPARE, node);

        // Send to other acceptors
        for (PaxosNode otherNode : node.getOtherNodes()) {
            ProposalContext context = contextStore.get(request.getProposalId());
            // Skip this request if consensus already achieved
            if (context.isCommitConsensus()) return;
            sendRequestWithRetries(request, PaxosMessageType.PREPARE, otherNode);
        }
    }

    /**
     * Sends ACCEPT requests to all Acceptors that have previously promised to accept a proposal,
     * attempting to finalize the consensus.
     */
    public void sendAcceptRequests(PaxosMessage request) throws RemoteException {
        UUID proposalId = request.getProposalId();
        Map<PaxosNode, Boolean> committedNodes = contextStore.get(proposalId).getCommittedNodes();

        // Send accept request to committed nodes that ever replied
        for (PaxosNode remoteNode : committedNodes.keySet()) {
            sendRequestWithRetries(request, PaxosMessageType.ACCEPT, remoteNode);
        }
    }

    /**
     * Processes COMMIT replies from Acceptors.
     * If a majority is reached, moves to the ACCEPT phase.
     */
    public void handleCommitReply(PaxosMessage reply) throws RemoteException {
        ServerLogger.log(node.getNodeId(), "Proposer - Received COMMIT rely from acceptor " + reply.getFromNode().getNodeId() + ":\n" + reply);

        UUID proposalId = reply.getProposalId();
        ProposalContext context = contextStore.get(proposalId);

        // Skip this reply if consensus already achieved
        if (context.isCommitConsensus()) {
            ServerLogger.log(node.getNodeId(), "Proposer - Reply ignored: consensus already achieved");
            return;
        }

        if (reply.getProposalNumber().compareTo(context.getLargestProposalNumber()) > 0) {
            context.setLargestProposalNumber(reply.getProposalNumber());
        }

        context.addCommittedNode(reply.getFromNode());
        node.updateContextStore(proposalId, context);

        if (context.achieveMajorityCommitted(node.getOtherNodes().size() + 1)) {
            ServerLogger.log(node.getNodeId(), "Proposer - Achieved consensus! Received COMMIT replies from majority of acceptors");
            context.setCommitConsensus(true);
            node.updateContextStore(proposalId, context);

            PaxosMessage acceptRequest = new PaxosMessage(
                    proposalId,
                    PaxosMessageType.ACCEPT,
                    context.getLargestProposalNumber(),
                    node,
                    node, // can be changed when sending request
                    reply.getOperationType(),
                    reply.getKey(),
                    reply.getValue()
            );
            sendAcceptRequests(acceptRequest);
        }
    }

    /**
     * Handles REJECT responses from Acceptors,
     * potentially leading to retries with higher proposal numbers to overcome conflicts.
     */
    public void handleRejectReply(PaxosMessage reply) throws RemoteException {
        ServerLogger.log(node.getNodeId(), "Proposer - Received REJECT rely from acceptor " + reply.getFromNode().getNodeId() + ":\n" + reply);

        UUID proposalId = reply.getProposalId();
        ProposalContext context = contextStore.get(proposalId);

        // Skip this reply if consensus already achieved
        if (context.isCommitConsensus()) {
            ServerLogger.log(node.getNodeId(), "Proposer - Reply ignored: consensus already achieved");
            return;
        }

        // Context changes, clear committed nodes record
        context.clearCommittedNodes();

        ServerLogger.log(node.getNodeId(), "Proposer - Restart a proposal with a higher proposal number");
        // Retry with a larger proposal number
        ProposalNumber largerNumber = new ProposalNumber(
                reply.getProposalNumber().getSequenceNumber() + 1,
                node.getNodeId()
        );
        context.setLargestProposalNumber(largerNumber);
        node.updateContextStore(proposalId, context);

        PaxosMessage prepareRequest = new PaxosMessage(
                proposalId,
                PaxosMessageType.PREPARE,
                largerNumber,
                node,
                node, // can be changed when sending request
                reply.getOperationType(),
                reply.getKey(),
                reply.getValue()
        );
        sendPrepareRequests(prepareRequest);
    }

    /**
     * Attempts to send a request multiple times to handle transient failures,
     * ensuring robust communication in adverse conditions.
     */
    private void sendRequestWithRetries(PaxosMessage request, PaxosMessageType messageType, PaxosNode targetNode) throws RemoteException {
        int maxRetries = Integer.parseInt(System.getenv("MAX_RETRY_COUNT"));
        PaxosMessage retryRequest = new PaxosMessage(
                request.getProposalId(),
                messageType,
                request.getProposalNumber(),
                node,
                targetNode,
                request.getOperationType(),
                request.getKey(),
                request.getValue()
        );
        for (int retryCount = 0; retryCount < maxRetries; retryCount++) {
            try {
                ServerLogger.log(node.getNodeId(), String.format("Proposer - Sending %s request to acceptor %s:\n%s",
                        messageType, targetNode.getNodeId(), retryRequest));
                node.sendMessage(retryRequest);
                break;
            } catch (RemoteException e) {
                ServerLogger.log(node.getNodeId(), String.format("Proposer - RMI exception during send %s request to acceptor %s: %s",
                        messageType, targetNode.getNodeId(), e.getMessage()));
                if (retryCount == maxRetries - 1) {
                    ServerLogger.log(node.getNodeId(), String.format("Proposer - Failed to send %s request to %s after %s attempts",
                            messageType, targetNode.getNodeId(), retryCount + 1));
                }
            }
        }
    }
}
