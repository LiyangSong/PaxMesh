package server;

import java.io.*;

/**
 * Provide logging functionality for the server.
 */
public class ServerLogger {
    /**
     * Log a message to the console and to the log file of the server.
     */
    public static void log(String nodeId, String message) {
        String logMessage;
        if (nodeId == null) {
            logMessage = message;
        } else {
            logMessage = String.format("[Node %s] %s", nodeId, message);
        }

        System.out.println(logMessage);

        try (FileWriter fw = new FileWriter(nodeId + ".log", true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            out.println(logMessage);
        } catch (IOException e) {
            System.err.println("Error writing to log file: " + e.getMessage());
        }
    }
}
