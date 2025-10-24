package com.liyang.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A thread-safe key-value store using ConcurrentHashMap for storing data.
 */
public class KeyValueStore {
    private final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();

    /**
     * Store the key-value pair.
     */
    public void put(String key, String value) {
        store.put(key, value);

    }

    /**
     * Retrieve the value for the key, or a default message.
     */
    public String get(String key) {
        return store.getOrDefault(key, "Key not found");
    }

    /**
     * Remove the key-value pair if the key exists.
     */
    public void delete(String key) {
        store.remove(key);
    }

    /**
     * Retrieve all key-value pairs in the store.
     */
    public List<String> getAll() {
        List<String> keyValuePairs = new ArrayList<>();
        for (Map.Entry<String, String> entry : store.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            keyValuePairs.add(key + "->" + value);
        }
        return keyValuePairs;
    }
}
