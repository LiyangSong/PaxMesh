package client;

import paxosInterface.PaxosNode;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Entry point for the client application.
 * Parses command-line arguments to configure and start a client instance.
 * Initialize RMI connection to perform concurrent requests to the server.
 */
public class ClientApp {
    public static void main(String[] args) {
        // Check for correct number of arguments
        if (args.length < 1 || args.length > 2 || (args.length == 2 && !args[1].equalsIgnoreCase("-t"))) {
            ClientLogger.log(null, "Usage: java client.ClientApp <node ID> [-t]");
            return;
        }

        String nodeId = args[0];
        ConcurrentHashMap<String, String> nodeRmiAddresses = getAddresses();
        String nodeRmiAddress = nodeRmiAddresses.get(nodeId);

        if (nodeRmiAddress == null) {
            ClientLogger.log(null, "Cannot find valid node RMI address with given nodeId");
            return;
        }

        boolean testFlag = args.length == 2 && args[1].equalsIgnoreCase("-t");

        try {
            // Build RMI connection through remote interface name
            PaxosNode node = (PaxosNode) Naming.lookup(nodeRmiAddress);
            ClientLogger.log(nodeId, "Client obtained RMI reference to server: " + nodeRmiAddress);
            ClientLogger.log(node.getNodeId(), "Acquired PAXOS node connection:\n" +
                    "Current node: " + node.getNodeId() + "\n" +
                    "Other nodes: " + node.getOtherNodes().stream().map(n -> {
                        try {
                            return n.getNodeId();
                        } catch (RemoteException e) {
                            return null;
                        }
                    }).filter(Objects::nonNull).toList()
            );

            if (!testFlag) {
                Client client = new Client(node);
                client.performAutomatedRequests();

                // Check current status of the keyValueStore
                ClientLogger.log(nodeId, "Current keyValueStore:\n" + node.getAll().toString());
                client.enterInteractiveMode();
            } else {
                // Test one proposer
                testOneProposer("key_test_1", "value_test_1", node);

                // Test two concurrent proposers in one proposal process
                testConcurrentProposals(
                        "key_test_2",
                        "value_test_2",
                        "value_test_50",
                        node,
                        node.getOtherNodes().get(1)
                );
            }

        } catch (Exception e) {
            ClientLogger.log(nodeId, "Client exception: " + e.getMessage());
        }
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

    /**
     * Single proposer test.
     */
    public static void testOneProposer(String key, String value, PaxosNode node) throws RemoteException {
        UUID proposalId = UUID.randomUUID();
        Client client = new Client(node);

        ClientLogger.log(node.getNodeId(), "==========Test PUT Operation by One Proposer in PAXOS==========");
        client.performPutRequest(proposalId, key, value);
        ClientLogger.log(node.getNodeId(), "Current key value pairs:\n" + node.getAll().toString());

        ClientLogger.log(node.getNodeId(), "==========Test GET Operation==========");
        client.performGetRequest(key);

        ClientLogger.log(node.getNodeId(), "==========Test DELETE Operation by One Proposer in PAXOS==========");
        client.performDeleteRequest(proposalId, key);
        ClientLogger.log(node.getNodeId(), "Current key value pairs:\n" + node.getAll().toString());
    }

    /**
     * Double concurrent proposers test.
     */
    public static void testConcurrentProposals(String key, String value1, String value2, PaxosNode node1, PaxosNode node2) throws Exception {
        UUID proposalId = UUID.randomUUID();

        // Start first proposal on a separate thread
        Thread t1 = new Thread(() -> {
            try {
                Client client1 = new Client(node1);
                client1.performPutRequest(proposalId, key, value1);
            } catch (RemoteException e) {
                throw new RuntimeException(e);
            }
        });

        // Start second proposal on a separate thread
        Thread t2 = new Thread(() -> {
            try {
                Client client2 = new Client(node2);
                client2.performPutRequest(proposalId, key, value2);
            } catch (RemoteException e) {
                throw new RuntimeException(e);
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        // Retrieve the value after the operations
        String finalValueNode1 = node1.handleGetRequest(key);
        String finalValueNode2 = node2.handleGetRequest(key);

        // Assert that both nodes have the same final value
        ClientLogger.log(node1.getNodeId(), "Final value on Node1: " + finalValueNode1);
        ClientLogger.log(node2.getNodeId(), "Final value on Node2: " + finalValueNode2);
    }
}
