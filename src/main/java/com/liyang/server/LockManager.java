package com.liyang.server;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles the acquisition and release of locks on keys to ensure that
 * concurrent operations on the same key are executed safely.
 */
public class LockManager {
    private final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> lockStatus = new ConcurrentHashMap<>();

    /**
     * Attempts to acquire a lock on a given key.
     * If the lock is already held, it waits for the specified timeout for the lock to become available.
     */
    public Object acquireLock(String key, long timeout) {
        // Create a new lock if not exist
        Object lock = locks.computeIfAbsent(key, k -> new Object());

        synchronized (lock) {
            long endTime = System.currentTimeMillis() + timeout;
            while (lockStatus.getOrDefault(key, false)) {
                long waitTime = endTime - System.currentTimeMillis();
                if (waitTime <= 0) {return null;}
                try {
                    lock.wait(waitTime);
                } catch(InterruptedException e) {
                    // Returns null if the timeout expires or thread is interrupted
                    Thread.currentThread().interrupt();
                    return null;
                }
            }

            lockStatus.put(key, true); // Mark the lock as held
            return locks.get(key);
        }
    }

    /**
     * Releases the lock held on a given key and notifies all threads waiting for this lock.
     */
    public void releaseLock(String key) {
        Object lock = locks.get(key);
        if (lock != null) {
            synchronized (lock) {
                lockStatus.put(key, false);
                // Once a lock is released, other threads waiting on the lock can proceed
                lock.notifyAll();
            }
        }
    }
}
