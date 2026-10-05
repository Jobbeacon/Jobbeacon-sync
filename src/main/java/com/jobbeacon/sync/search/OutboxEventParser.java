package com.jobbeacon.sync.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Optional;
import java.util.Set;

public final class OutboxEventParser {

    private static final Set<String> APPLIED_ENVELOPE_OPS = Set.of("c", "u", "r");
    private static final Set<String> APPLIED_ACTIONS = Set.of("c", "u", "d");

    private final ObjectMapper objectMapper;

    public OutboxEventParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public Optional<OutboxChange> parse(String value) {
        if (value == null || value.isBlank()) {
            return Optional.empty();
        }
        try {
            JsonNode root = objectMapper.readTree(value);
            JsonNode envelope = unwrap(root);
            String envelopeOp = text(envelope.get("op"));
            if (envelopeOp == null || !APPLIED_ENVELOPE_OPS.contains(envelopeOp)) {
                return Optional.empty();
            }
            JsonNode source = envelope.get("source");
            if (source != null && "debezium_heartbeat".equals(text(source.get("table")))) {
                return Optional.empty();
            }
            JsonNode after = envelope.get("after");
            if (after == null || after.isNull()) {
                return Optional.empty();
            }
            String jobId = text(after.get("job_id"));
            String action = text(after.get("op"));
            if (jobId == null || action == null || !APPLIED_ACTIONS.contains(action)) {
                return Optional.empty();
            }
            JsonNode payload = "d".equals(action) ? null : document(after.get("payload"));
            if (!"d".equals(action) && (payload == null || payload.isNull())) {
                throw new IllegalStateException("Search outbox row " + jobId + " has no payload");
            }
            return Optional.of(new OutboxChange(jobId, action, payload));
        } catch (IllegalStateException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IllegalStateException("Cannot parse search outbox event", exception);
        }
    }

    private JsonNode unwrap(JsonNode root) {
        if (root.has("schema") && root.has("payload") && root.get("payload").isObject()) {
            return root.get("payload");
        }
        return root;
    }

    private JsonNode document(JsonNode payload) throws Exception {
        if (payload == null || payload.isNull()) {
            return null;
        }
        if (payload.isTextual()) {
            return objectMapper.readTree(payload.asText());
        }
        return payload;
    }

    private String text(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        String value = node.asText();
        return value == null || value.isBlank() ? null : value;
    }
}
