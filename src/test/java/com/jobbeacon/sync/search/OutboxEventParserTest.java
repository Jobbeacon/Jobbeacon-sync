package com.jobbeacon.sync.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxEventParserTest {

    private final OutboxEventParser parser = new OutboxEventParser(new ObjectMapper());

    @Test
    void readsInsertPayloadAndIgnoresHeartbeatAndOutboxDelete() {
        Optional<OutboxChange> created = parser.parse("""
                {
                  "op": "c",
                  "after": {
                    "job_id": "01990000-0000-7000-8000-000000000001",
                    "op": "c",
                    "payload": { "title": "Backend Java" }
                  },
                  "source": { "table": "job_search_outbox" }
                }
                """);
        assertThat(created).isPresent();
        assertThat(created.orElseThrow().jobId()).isEqualTo("01990000-0000-7000-8000-000000000001");
        assertThat(created.orElseThrow().payload().get("title").asText()).isEqualTo("Backend Java");

        assertThat(parser.parse("""
                {"op":"c","after":{"id":1},"source":{"table":"debezium_heartbeat"}}
                """)).isEmpty();
        assertThat(parser.parse("""
                {"op":"d","before":{"job_id":"01990000-0000-7000-8000-000000000001","op":"c"},"after":null}
                """)).isEmpty();
    }

    @Test
    void keepsTheLatestChangeForEachJob() {
        List<OutboxChange> latest = OutboxBatch.latestPerJob(List.of(
                new OutboxChange("job-1", "c", null),
                new OutboxChange("job-1", "d", null),
                new OutboxChange("job-2", "u", null)
        ));

        assertThat(latest).containsExactly(
                new OutboxChange("job-1", "d", null),
                new OutboxChange("job-2", "u", null)
        );
    }
}
