package com.liyang.server;

import lombok.Getter;

import java.io.Serializable;


/**
 * Represents an operation in the system, containing details like the operation type and the key-value pair involved.
 */
@Getter
public class Operation implements Serializable {
    private final OperationType operationType;
    private final String key;
    private final String value;

    public Operation(OperationType operationType, String key, String value) {
        this.operationType = operationType;
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return "Operation{" +
                "operationType=" + operationType +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
