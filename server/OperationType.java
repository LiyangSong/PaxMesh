package server;

public enum OperationType {
    GET("GET"),
    PUT("PUT"),
    DELETE("DELETE");

    private final String type;

    OperationType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return type;
    }
}
