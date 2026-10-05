package com.jobbeacon.sync.search;

import com.fasterxml.jackson.databind.JsonNode;

public record OutboxChange(String jobId, String action, JsonNode payload) {

    public boolean delete() {
        return "d".equals(action);
    }
}
