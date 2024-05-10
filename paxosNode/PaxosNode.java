package paxosInterface;

import server.PaxosMessage;
import server.ProposalContext;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface PaxosNode extends Remote {
    String getNodeId() throws RemoteException;

    List<PaxosNode> getOtherNodes() throws RemoteException;

    Map<UUID, ProposalContext> getContextStore() throws RemoteException;

    void setOtherNodes(List<PaxosNode> otherNodes) throws RemoteException;

    String handlePutRequest(UUID proposalId, String key, String value) throws RemoteException;

    String handleGetRequest(String key) throws RemoteException;

    String handleDeleteRequest(UUID proposalId, String key) throws RemoteException;

    void sendMessage(PaxosMessage message) throws RemoteException;

    void receiveMessage(PaxosMessage message) throws RemoteException;

    void updateContextStore(UUID proposalId, ProposalContext context) throws RemoteException;

    List<String> getAll() throws RemoteException;
}
