package server;

import paxosInterface.PaxosNode;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;


/**
 * This class implements the PaxosNode interface
 * and encapsulates the functionality of a node in a Paxos cluster,
 * handling the orchestration of proposals, acceptances, and learning of new operations.
 */
public class PaxosNodeImpl extends UnicastRemoteObject implements PaxosNode {
    private final String nodeId;
    private List<PaxosNode> otherNodes;
    private final Map<UUID, ProposalContext> contextStore = new ConcurrentHashMap<>();
    private final Proposer proposer = new Proposer(this, contextStore);
    private final Acceptor acceptor = new Acceptor(this, contextStore);
    private final Learner learner = new Learner(this, contextStore);
    private final KeyValueStore keyValueStore = new KeyValueStore();
    private final LockManager lockManager = new LockManager();

    public PaxosNodeImpl(String nodeId, List<PaxosNode> otherNodes) throws RemoteException {
        super();
        this.nodeId = nodeId;
        this.otherNodes = otherNodes;
    }

    @Override
    public String getNodeId() throws RemoteException{
        return nodeId;
    }

    @Override
    public List<PaxosNode> getOtherNodes() throws RemoteException{
        return otherNodes;
    }

    @Override
    public Map<UUID, ProposalContext> getContextStore() throws RemoteException {
        return contextStore;
    }

    @Override
    public void setOtherNodes(List<PaxosNode> otherNodes) throws RemoteException{
        this.otherNodes = otherNodes;
    }

    /**
     * This method handle client requests to put values in the key-value store
     * using the Paxos protocol to ensure consensus.
     */
    @Override
    public String handlePutRequest(UUID proposalId, String key, String value) throws RemoteException {
        PaxosMessage putRequest = proposer.initiateProposal(
                proposalId, OperationType.PUT, key, value
        );
        getConsensus(putRequest);
        return "Succeed to perform PUT " + key + " " + value;
    }

    /**
     * This method handle client requests to get values in the key-value store
     * using the Paxos protocol to ensure consensus.
     */
    @Override
    public String handleGetRequest(String key) throws RemoteException {
        return "Succeed to perform GET " + key + ": " + keyValueStore.get(key);
    }

    /**
     * This method handle client requests to delete values in the key-value store
     * using the Paxos protocol to ensure consensus.
     */
    @Override
    public String handleDeleteRequest(UUID proposalId, String key) throws RemoteException {
        PaxosMessage deleteRequest = proposer.initiateProposal(
                proposalId, OperationType.DELETE, key, null
        );
        getConsensus(deleteRequest);
        return "Succeed to perform DELETE " + key;
    }

    /**
     * Orchestrates the consensus process for a given operation by initiating the proposal process,
     * waiting for a majority agreement, and applying the operation.
     */
    public void getConsensus(PaxosMessage request) throws RemoteException {
        CompletableFuture<Void> operationFuture = CompletableFuture.runAsync(() -> {
            try {
                proposer.sendPrepareRequests(request);  // Initiate the proposal
            } catch (Exception e) {
                ServerLogger.log(nodeId, "Error during achieving the PAXOS consensus: " + e.getMessage());
                throw new CompletionException(e);
            }
        }).thenRun(() -> {
            Operation finalOperation = contextStore.get(request.getProposalId()).getFinalOperation(); // Result of the proposal
            ServerLogger.log(nodeId, "Final operation from learner that achieved consensus: " + finalOperation);

            if (finalOperation == null) {
                ServerLogger.log(nodeId, "Failed to get a final operation consensus due to simulated random errors in acceptors");
                return;
            }

            try { // Perform the proposal result operation
                long timeout = Long.parseLong(System.getenv("ACQUIRE_LOCK_TIME_OUT"));
                if (lockManager.acquireLock(finalOperation.getKey(), timeout) != null) {
                    try {
                        if (request.getOperationType() == OperationType.PUT) {
                            keyValueStore.put(finalOperation.getKey(), finalOperation.getValue());
                        }
                        if (request.getOperationType() == OperationType.DELETE) {
                            keyValueStore.delete(finalOperation.getKey());
                        }
                    } finally {
                        lockManager.releaseLock(finalOperation.getKey());
                    }
                } else {
                    ServerLogger.log(nodeId, "Failed to acquire lock within timeout");
                    throw new IllegalStateException();
                }
            } catch (Exception e) {
                ServerLogger.log(nodeId, "Error executing the PAXOS consensus: " + e.getMessage());
                throw new CompletionException(e);
            }
        });

        try {
            operationFuture.get(10, TimeUnit.SECONDS); // wait for consensus with a timeout
        } catch (Exception e) {
            ServerLogger.log(nodeId, "Error process PAXOS consensus: " + e.getMessage());
            throw new RemoteException("Error process PAXOS consensus: " + e.getMessage());
        }
    }

    /**
     * Facilitates sending messages between nodes to propagate the Paxos protocol messages.
     */
    @Override
    public void sendMessage(PaxosMessage message) throws RemoteException {
        message.getToNode().receiveMessage(message);
    }

    /**
     * Facilitates receiving messages between nodes to propagate the Paxos protocol messages.
     */
    @Override
    public void receiveMessage(PaxosMessage message) throws RemoteException {
        switch (message.getMessageType()) {
            case PREPARE -> acceptor.handlePrepareRequest(message);
            case COMMIT -> proposer.handleCommitReply(message);
            case REJECT -> proposer.handleRejectReply(message);
            case ACCEPT -> acceptor.handleAcceptRequest(message);
            case ACCEPTED -> learner.handleAcceptedNotification(message);
        }
    }

    @Override
    public String toString() {
        return nodeId;
    }

    @Override
    public void updateContextStore(UUID proposalId, ProposalContext context) {
        contextStore.put(proposalId, context);
    }

    @Override
    public List<String> getAll() throws RemoteException {
        return keyValueStore.getAll();
    }

}
