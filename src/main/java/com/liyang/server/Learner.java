package com.liyang.server;

import java.rmi.RemoteException;
import java.util.*;


/**
 * Responsible for finalizing the consensus process by receiving notifications of accepted proposals,
 * confirming when a majority is achieved, and ensuring the system learns the agreed-upon values.
 */
public class Learner {
    private final PaxosNodeImpl node;
    private final Map<UUID, ProposalContext> contextStore;

    public Learner(PaxosNodeImpl node, Map<UUID, ProposalContext> contextStore) {
        this.node = node;
        this.contextStore = contextStore;
    }

    /**
     * Handles notifications of accepted proposals,
     * verifying if a majority has been reached and setting the final operation accordingly.
     */
    public void handleAcceptedNotification(PaxosMessage notification) throws RemoteException {

        ServerLogger.log(node.getNodeId(), "Learner - Received ACCEPTED notification from acceptor " + notification.getFromNode().getNodeId() + ":\n" + notification);

        UUID proposalId = notification.getProposalId();
        ProposalContext context = contextStore.get(proposalId);
        context.addAcceptedNode(notification.getFromNode());
        node.updateContextStore(proposalId, context);

        // Check if this acceptance leads to a majority
        if (context.achieveMajorityAccepted(node.getOtherNodes().size() + 1)) {
            ServerLogger.log(node.getNodeId(), "Learner - Achieved consensus! Received ACCEPTED notifications from majority of acceptors");
            context.setAcceptedConsensus(true);

            Operation finalOperation = new Operation(
                    notification.getOperationType(),
                    notification.getKey(),
                    notification.getValue()
            );
            context.setFinalOperation(finalOperation);
            node.updateContextStore(proposalId, context);
        }
    }
}
