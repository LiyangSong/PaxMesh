package server;

import paxosInterface.PaxosNode;

import java.rmi.RemoteException;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.StreamSupport;


/**
 * Represents an Acceptor in the Paxos protocol,
 * responsible for accepting or rejecting proposals based on the Paxos consensus rules.
 */
public class Acceptor {
    private final PaxosNodeImpl node;
    private final Map<UUID, ProposalContext> contextStore;
    private final Random random = new Random();

    public Acceptor(PaxosNodeImpl node, Map<UUID, ProposalContext> contextStore) {
        this.node = node;
        this.contextStore = contextStore;
    }

    /**
     * Processes PREPARE requests from Proposers.
     * If the proposal number is higher than any previously promised,
     * it promises not to accept lower numbered proposals.
     */
    public void handlePrepareRequest(PaxosMessage request) throws RemoteException {

        ServerLogger.log(node.getNodeId(), "Acceptor - Received PREPARE request from proposer " + request.getFromNode().getNodeId() + ":\n" + request);

        maybeFail();  // Simulate potential failure before handling prepare request

        UUID proposalId = request.getProposalId();
        ProposalNumber proposalNumber = request.getProposalNumber();
        ProposalContext context = contextStore.get(proposalId);
        ProposalNumber promisedNumber = context.getPromisedProposalNumber();

        PaxosMessage reply;
        String log;
        if (promisedNumber == null || proposalNumber.compareTo(promisedNumber) > 0) {
            context.setPromisedProposalNumber(proposalNumber);
            node.updateContextStore(proposalId, context);

            reply = new PaxosMessage(
                    proposalId,
                    PaxosMessageType.COMMIT,
                    proposalNumber,
                    node,
                    request.getFromNode(),
                    request.getOperationType(),
                    request.getKey(),
                    request.getValue()
            );
            log = "Acceptor - Proposal number higher than current promised number. Sending COMMIT reply to proposer " + request.getFromNode().getNodeId()+ ":\n" + reply;
        } else {
            reply = new PaxosMessage(
                    proposalId,
                    PaxosMessageType.REJECT,
                    promisedNumber,  // Reply with the promised higher number
                    node,
                    request.getFromNode(),
                    request.getOperationType(),
                    request.getKey(),
                    request.getValue()
            );
            log = "Acceptor - Proposal number smaller than or equal to current promised number. Sending REJECT reply to proposer " + request.getFromNode().getNodeId()+ ":\n" + reply;
        }

        ServerLogger.log(node.getNodeId(), log);
        node.sendMessage(reply);
    }

    /**
     * Handles ACCEPT requests.
     * If the proposal number matches or exceeds the promised number,
     * it accepts the proposal and notifies learners.
     */
    public void handleAcceptRequest(PaxosMessage request) throws RemoteException {

        ServerLogger.log(node.getNodeId(), "Acceptor - Received ACCEPT request from proposal " + request.getFromNode().getNodeId() + ":\n" + request);

        maybeFail();  // Simulate potential failure before handling accept request

        UUID proposalId = request.getProposalId();
        ProposalNumber proposalNumber = request.getProposalNumber();

        ProposalContext context = contextStore.get(proposalId);
        ProposalNumber promisedNumber = context.getPromisedProposalNumber();

        if (promisedNumber == null || proposalNumber.compareTo(promisedNumber) >= 0) {
            ServerLogger.log(node.getNodeId(), "Acceptor - Proposal number higher than or equal to current promised number. Accepted proposal, send ACCEPTED notification to all learners");

            context.setPromisedProposalNumber(proposalNumber);  // Update the promised number to the current
            node.updateContextStore(proposalId, context);

            PaxosMessage notification = new PaxosMessage(
                    proposalId,
                    PaxosMessageType.ACCEPTED,
                    request.getProposalNumber(),
                    node,
                    node,  // Send to self learner, can be changed in sendAcceptedNotification
                    request.getOperationType(),
                    request.getKey(),
                    request.getValue()
            );
            sendAcceptedNotification(notification);
        }
    }

    /**
     * Notifies all learners that a proposal has been accepted,
     * ensuring they are aware of the latest state agreed upon by the acceptors.
     */
    private void sendAcceptedNotification(PaxosMessage notification) throws RemoteException {
        maybeFail();  // Simulate potential failure before sending accepted notification

        // Notify learner of self node
        ServerLogger.log(node.getNodeId(), "Acceptor - Sending ACCEPTED notification to learner of self node:\n" + notification);
        node.sendMessage(notification);

        // Notify other learners
        for (PaxosNode otherNode : node.getOtherNodes()) {
            PaxosMessage otherNotification = new PaxosMessage(
                    notification.getProposalId(),
                    PaxosMessageType.ACCEPTED,
                    notification.getProposalNumber(),
                    node,
                    otherNode,
                    notification.getOperationType(),
                    notification.getKey(),
                    notification.getValue()
            );
            try {
                ServerLogger.log(node.getNodeId(), "Acceptor - Sending ACCEPTED notification to learner " + otherNode.getNodeId() + ":\n" + otherNotification);
                node.sendMessage(otherNotification);
            } catch (RemoteException e) {
                ServerLogger.log(node.getNodeId(), "Acceptor - RMI exception during send ACCEPTED notification to learner " + otherNode.getNodeId() + ": " + e.getMessage());
            }
        }
    }

    /**
     * Simulates failures to mimic real-world scenarios where network or system issues may prevent an acceptor from responding.
     */
    private void maybeFail() throws RemoteException {
        if (random.nextDouble() < Double.parseDouble(System.getenv("FAILURE_RATE"))) {  // random failure chance
            throw new RemoteException("SIMULATED ACCEPTOR FAILURE");
        }
    }
}
