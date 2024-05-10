package server;

import paxosInterface.PaxosNode;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The main class for starting the server application.
 * Initializes and starts RMI servers based on configuration obtained from environment variables.
 * Dynamically creates registry instances for each server and binds server instances to the RMI registry.
 */
public class ServerApp {
    public static void main(String[] args) {
        ConcurrentHashMap<String, String> nodeRmiAddresses = getAddresses();
        List<String> nodeIds = new ArrayList<>(nodeRmiAddresses.keySet());

        // Start each node
        nodeRmiAddresses.forEach((nodeId, nodeRmiAddress) -> {
            try {
                String[] addressParts = nodeRmiAddress.split(":");
                String[] portAndName = addressParts[2].split("/");
                int nodePort = Integer.parseInt(portAndName[0]);
                LocateRegistry.createRegistry(nodePort);
                ServerLogger.log(null, "PAXOS node " + nodeId + " RMI registry created on port " + nodePort);

                PaxosNode node = new PaxosNodeImpl(nodeId, null); // will connect with other nodes later
                Naming.rebind(nodeRmiAddress, node);
                ServerLogger.log(nodeId,"PAXOS node RMI instance name bound: " + nodeRmiAddress);

            } catch(Exception e) {
                ServerLogger.log(nodeId, "Server exception when building RMI system: " + e.getMessage());
            }
        });

        // Initialize node connections
        nodeRmiAddresses.forEach((nodeId, nodeRmiAddress) -> {
            try {
                PaxosNode node = (PaxosNode) Naming.lookup(nodeRmiAddress);
                List<PaxosNode> otherNodes = nodeIds.stream()
                        .filter(id -> !id.equals(nodeId))
                        .map(id -> {
                            try {
                                return (PaxosNode) Naming.lookup(nodeRmiAddresses.get(id));
                            } catch (Exception e) {
                                ServerLogger.log(nodeId, "Server exception when building RMI system: " + e.getMessage());
                                return null;
                            }
                        }).filter(Objects::nonNull).toList();
                node.setOtherNodes(otherNodes);
                ServerLogger.log(nodeId, "PAXOS node connections updated for " + nodeId);
                ServerLogger.log(nodeId,"PAXOS node " + nodeId + " ready");
            } catch (Exception e) {
                ServerLogger.log(nodeId, "Server exception when setting other nodes for " + nodeId + ": " + e.getMessage());
            }
        });
    }

    /**
     * Retrieves server configuration from environment variables.
     * Constructs RMI addresses for each server and returns them as a ConcurrentHashMap.
     */
    private static ConcurrentHashMap<String, String> getAddresses() {
        ConcurrentHashMap<String, String> nodeRmiAddresses = new ConcurrentHashMap<>();

        for (int i = 1; i <= Integer.parseInt(System.getenv("NODE_NUM")); i++) {
            String nodeHost = System.getenv(String.format("NODE_HOST_%d", i));
            String nodePort = System.getenv(String.format("NODE_PORT_%d", i));
            String nodeId = nodePort;
            String nodeRmiAddress = String.format("rmi://%s:%s/%s", nodeHost, nodePort, nodeId);
            nodeRmiAddresses.put(nodeId, nodeRmiAddress);
        }

        return nodeRmiAddresses;
    }
}
