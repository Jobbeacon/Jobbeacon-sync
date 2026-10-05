package com.jobbeacon.sync.engine;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DebeziumEngineRunnerTest {

    @Test
    void readsHostPortAndDatabaseFromJdbcUrl() {
        String url = "jdbc:postgresql://jobbeacon-db:5433/job_platform?sslmode=disable";

        assertThat(DebeziumEngineRunner.host(url)).isEqualTo("jobbeacon-db");
        assertThat(DebeziumEngineRunner.port(url)).isEqualTo("5433");
        assertThat(DebeziumEngineRunner.databaseName(url)).isEqualTo("job_platform");
    }
}
