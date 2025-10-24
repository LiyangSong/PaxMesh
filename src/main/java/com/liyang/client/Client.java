package com.liyang.client;

import com.liyang.paxosNode.PaxosNode;

import java.rmi.RemoteException;
import java.util.Random;
import java.util.Scanner;
import java.util.UUID;


/**
 * Client class orchestrates the operations of the client, managing automated requests and interactive mode.
 */
public class Client {
    private final PaxosNode node;

    /**
     * Initialize the client with the RMI remote object.
     */
    public Client(PaxosNode node) {
        this.node = node;
    }

    /**
     * Perform automated PUT, GET, and DELETE operations.
     */
    public void performAutomatedRequests() throws RemoteException {
        Random random = new Random();

        // Pre-population some data into the KeyValue store
        for (int i = 0; i < Integer.parseInt(System.getenv("PRE_POPULATE_OPERATIONS")); i++) {
            UUID proposalId = UUID.randomUUID();
            String value = generateRandomString(random, Integer.parseInt(System.getenv("RANDOM_VALUE_LENGTH")));
            performPutRequest(proposalId, "key_" + i, "value_" + value);
        }

        // Perform 5 PUT operations
        for (int i = 0; i < Integer.parseInt(System.getenv("PUT_OPERATIONS")); i++) {
            UUID proposalId = UUID.randomUUID();
            String value = generateRandomString(random, Integer.parseInt(System.getenv("RANDOM_VALUE_LENGTH")));
            performPutRequest(proposalId, "key_" + i, "value_" + value);
        }

        // Perform 5 GET operations
        for (int i = 0; i < Integer.parseInt(System.getenv("GET_OPERATIONS")); i++) {
            performGetRequest("key_" + i);
        }

        // Perform 5 DELETE operations
        for (int i = 0; i < Integer.parseInt(System.getenv("DELETE_OPERATIONS")); i++) {
            UUID proposalId = UUID.randomUUID();
            performDeleteRequest(proposalId, "key_" + i);
        }
    }

    /**
     * Enter interactive mode allowing the user to manually input commands to PUT, GET, or DELETE.
     */
    public void enterInteractiveMode() throws RemoteException {
        Scanner scanner = new Scanner(System.in);
        String input = "";
        while (!"exit".equalsIgnoreCase(input)) {
            System.out.println("Enter request (PUT <key> <value>, GET <key>, DELETE <key>, 'print' to print all key value pairs, 'exit' to quit):");
            input = scanner.nextLine();
            String inputUpperCase = input.toUpperCase();

            if (inputUpperCase.startsWith("PUT")) {
                String[] parts = input.split(" ", 3);
                if (parts.length == 3) {
                    UUID proposalId = UUID.randomUUID();
                    performPutRequest(proposalId, parts[1], parts[2]);
                } else {
                    ClientLogger.log(node.getNodeId(), "Invalid PUT syntax");
                }

            } else if (inputUpperCase.startsWith("GET")) {
                String[] parts = input.split(" ", 2);
                if (parts.length == 2) {
                    performGetRequest(parts[1]);
                } else {
                    ClientLogger.log(node.getNodeId(), "Invalid GET syntax");
                }

            } else if (inputUpperCase.startsWith("DELETE")) {
                String[] parts = input.split(" ", 2);
                if (parts.length == 2) {
                    UUID proposalId = UUID.randomUUID();
                    performDeleteRequest(proposalId, parts[1]);
                } else {
                    ClientLogger.log(node.getNodeId(), "Invalid DELETE syntax");
                }
            } else if (inputUpperCase.equals("PRINT")) {
                ClientLogger.log(node.getNodeId(), "Current keyValueStore:\n" + node.getAll().toString());
            } else if (!"exit".equalsIgnoreCase(input)) {
                ClientLogger.log(node.getNodeId(), "Unknown command");
            }
        }
        scanner.close();
    }

    /**
     * Generate a random alphanumeric string of a given length.
     */
    private String generateRandomString(Random random, int stringLength) {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        return random.ints(leftLimit, rightLimit + 1)
                .limit(stringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    /**
     * Perform PUT operation invoking method of RMI object
     */
    void performPutRequest(UUID proposalId, String key, String value) throws RemoteException {
        try {
            String reply = node.handlePutRequest(proposalId, key, value);
            ClientLogger.log(node.getNodeId(), reply);
        } catch (Exception e) {
            ClientLogger.log(node.getNodeId(), String.format("Error during PUT operation for %s with %s: %s", key, value, e.getMessage()));
        }
    }

    /**
     * Perform GET operation invoking method of RMI object
     */
    void performGetRequest(String key) throws RemoteException {
        try {
            String reply = node.handleGetRequest(key);
            ClientLogger.log(node.getNodeId(), reply);
        } catch (Exception e) {
            ClientLogger.log(node.getNodeId(), String.format("Error during GET operation for %s: %s", key, e.getMessage()));
        }
    }

    /**
     * Perform DELETE operation invoking method of RMI object
     */
    void performDeleteRequest(UUID proposalId, String key) throws RemoteException {
        try {
            String reply = node.handleDeleteRequest(proposalId, key);
            ClientLogger.log(node.getNodeId(), reply);
        } catch (Exception e) {
            ClientLogger.log(node.getNodeId(), String.format("Error during DELETE operation for %s: %s", key, e.getMessage()));
        }
    }
}
