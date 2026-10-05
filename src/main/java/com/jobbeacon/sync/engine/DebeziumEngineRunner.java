package com.jobbeacon.sync.engine;

import com.jobbeacon.sync.JobBeaconSyncApplication;
import com.jobbeacon.sync.config.SyncProperties;
import com.jobbeacon.sync.search.ElasticsearchIndexWriter;
import com.jobbeacon.sync.search.JobIndexInitializer;
import com.jobbeacon.sync.search.OutboxChange;
import com.jobbeacon.sync.search.OutboxEventParser;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@ConditionalOnProperty(name = "cdc.mode", havingValue = "stream", matchIfMissing = true)
public class DebeziumEngineRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DebeziumEngineRunner.class);

    private final SyncProperties properties;
    private final Environment environment;
    private final ElasticsearchIndexWriter indexWriter;
    private final JobIndexInitializer indexInitializer;
    private final OutboxEventParser parser;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private Thread engineThread;

    public DebeziumEngineRunner(
            SyncProperties properties,
            Environment environment,
            ElasticsearchIndexWriter indexWriter,
            JobIndexInitializer indexInitializer,
            com.fasterxml.jackson.databind.ObjectMapper objectMapper
    ) {
        this.properties = properties;
        this.environment = environment;
        this.indexWriter = indexWriter;
        this.indexInitializer = indexInitializer;
        this.parser = new OutboxEventParser(objectMapper);
    }

    @Override
    public void run(ApplicationArguments args) {
        engineThread = new Thread(this::loop, "job-search-cdc");
        engineThread.start();
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (engineThread != null) {
            engineThread.interrupt();
        }
    }

    private void loop() {
        Thread.currentThread().setContextClassLoader(JobBeaconSyncApplication.class.getClassLoader());
        while (running.get()) {
            try (DebeziumEngine<ChangeEvent<String, String>> engine = engine()) {
                indexInitializer.ensureAlias();
                log.info("Job search CDC is streaming {}", properties.getSlotName());
                engine.run();
            } catch (Exception exception) {
                if (!running.get()) {
                    return;
                }
                log.error("Job search CDC stopped. Retrying in 5 seconds", exception);
                sleep();
            }
        }
    }

    private DebeziumEngine<ChangeEvent<String, String>> engine() {
        return DebeziumEngine.create(Json.class)
                .using(connectorProperties())
                .notifying((records, committer) -> {
                    List<OutboxChange> changes = new ArrayList<>();
                    for (ChangeEvent<String, String> record : records) {
                        parser.parse(record.value()).ifPresent(changes::add);
                    }
                    try {
                        indexWriter.apply(changes);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw interrupted;
                    } catch (Exception exception) {
                        throw new IllegalStateException("Failed to index search outbox batch", exception);
                    }
                    for (ChangeEvent<String, String> record : records) {
                        committer.markProcessed(record);
                    }
                    committer.markBatchFinished();
                })
                .build();
    }

    private Properties connectorProperties() {
        String url = environment.getRequiredProperty("spring.datasource.url");
        String username = environment.getRequiredProperty("spring.datasource.username");
        String password = environment.getProperty("spring.datasource.password", "");
        String database = databaseName(url);

        Properties props = new Properties();
        props.setProperty("name", "jobbeacon-search-cdc");
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        props.setProperty("offset.storage", "io.debezium.storage.jdbc.offset.JdbcOffsetBackingStore");
        props.setProperty("offset.storage.jdbc.connection.url", url);
        props.setProperty("offset.storage.jdbc.connection.user", username);
        props.setProperty("offset.storage.jdbc.connection.password", password);
        props.setProperty("offset.storage.jdbc.table.name", "debezium_offset_storage");
        props.setProperty("offset.flush.interval.ms", "10000");
        props.setProperty("plugin.name", "pgoutput");
        props.setProperty("database.hostname", host(url));
        props.setProperty("database.port", port(url));
        props.setProperty("database.user", username);
        props.setProperty("database.password", password);
        props.setProperty("database.dbname", database);
        props.setProperty("topic.prefix", properties.getTopicPrefix());
        props.setProperty("slot.name", properties.getSlotName());
        props.setProperty("publication.name", properties.getPublicationName());
        props.setProperty("publication.autocreate.mode", "filtered");
        props.setProperty("table.include.list", "public.job_search_outbox,public.debezium_heartbeat");
        props.setProperty("tombstones.on.delete", "false");
        props.setProperty("heartbeat.interval.ms", "10000");
        props.setProperty(
                "heartbeat.action.query",
                "INSERT INTO debezium_heartbeat (id, ts) VALUES (1, NOW()) ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts"
        );
        props.setProperty("key.converter.schemas.enable", "false");
        props.setProperty("value.converter.schemas.enable", "false");
        props.setProperty("max.batch.size", "512");
        props.setProperty("poll.interval.ms", "1000");
        return props;
    }

    private void sleep() {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            running.set(false);
        }
    }

    static String databaseName(String jdbcUrl) {
        String withoutQuery = jdbcUrl.split("\\?")[0];
        return withoutQuery.substring(withoutQuery.lastIndexOf('/') + 1);
    }

    static String host(String jdbcUrl) {
        String body = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3);
        String hostPort = body.substring(0, body.indexOf('/'));
        return hostPort.split(":")[0];
    }

    static String port(String jdbcUrl) {
        String body = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3);
        String hostPort = body.substring(0, body.indexOf('/'));
        String[] parts = hostPort.split(":");
        return parts.length > 1 ? parts[1] : "5432";
    }
}
