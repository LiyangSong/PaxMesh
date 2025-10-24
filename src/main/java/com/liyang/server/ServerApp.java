package com.liyang.server;

import com.liyang.paxosNode.PaxosNode;
import com.liyang.registry.InMemoryRegistry;
import com.liyang.registry.NodeInfo;
import com.liyang.registry.Registry;

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
        // Discover topology via registry (env-backed in-memory)
        Registry registry = new InMemoryRegistry();
        List<NodeInfo> nodes;
        try {
            nodes = registry.discover();
        } catch (Exception e) {
            ServerLogger.log(null, "Failed to discover nodes from registry: " + e.getMessage());
            return;
        }

        ConcurrentHashMap<String, String> nodeRmiAddresses = new ConcurrentHashMap<>();
        List<String> nodeIds = new ArrayList<>();
        for (NodeInfo ni : nodes) {
            nodeIds.add(ni.getNodeId());
            nodeRmiAddresses.put(ni.getNodeId(), ni.rmiAddress());
        }

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
}
