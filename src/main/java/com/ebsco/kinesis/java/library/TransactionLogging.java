package com.ebsco.kinesis.java.library;

/**
 * Created by aganapathy on 4/30/17.
 */
public class TransactionLogging {

    private String sessionId;
    private String payload;

    public TransactionLogging(String sessionId, String payload) {
        this.sessionId = sessionId;
        this.payload = payload;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    @Override public String toString() {
        return "TransactionLogging{" + "sessionId='" + sessionId + '\'' + ", payload='" + payload + '\'' + '}';
    }
}
