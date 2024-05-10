package server;

public enum PaxosMessageType {
    PREPARE("PREPARE"),
    COMMIT("COMMIT"),
    REJECT("REJECT"),
    ACCEPT("ACCEPT"),
    ACCEPTED("ACCEPTED");

    private final String type;

    PaxosMessageType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return type;
    }
}
