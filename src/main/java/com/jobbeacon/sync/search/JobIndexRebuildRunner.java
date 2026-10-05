package com.jobbeacon.sync.search;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.GetAliasResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobbeacon.sync.config.SyncProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Set;

@Component
@ConditionalOnProperty(name = "cdc.mode", havingValue = "rebuild")
public class JobIndexRebuildRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(JobIndexRebuildRunner.class);
    private static final DateTimeFormatter SUFFIX = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private final ElasticsearchClient elasticsearchClient;
    private final ElasticsearchIndexWriter indexWriter;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final SyncProperties properties;
    private final ConfigurableApplicationContext context;

    public JobIndexRebuildRunner(
            ElasticsearchClient elasticsearchClient,
            ElasticsearchIndexWriter indexWriter,
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            SyncProperties properties,
            ConfigurableApplicationContext context
    ) {
        this.elasticsearchClient = elasticsearchClient;
        this.indexWriter = indexWriter;
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.context = context;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        String alias = properties.getElasticsearch().getIndex();
        String nextIndex = alias + "-" + LocalDateTime.now().format(SUFFIX);
        JobsIndex.create(elasticsearchClient, nextIndex);
        List<OutboxChange> changes = latestRows();
        indexWriter.indexAll(nextIndex, changes);
        Set<String> previous = indexesForAlias(alias);
        elasticsearchClient.indices().updateAliases(request -> {
            for (String index : previous) {
                request.actions(action -> action.remove(remove -> remove.index(index).alias(alias)));
            }
            request.actions(action -> action.add(add -> add.index(nextIndex).alias(alias).isWriteIndex(true)));
            return request;
        });
        for (String index : previous) {
            if (!index.equals(nextIndex)) {
                elasticsearchClient.indices().delete(request -> request.index(index));
            }
        }
        log.info("Rebuilt {} with {} searchable jobs on {}", alias, changes.size(), nextIndex);
        int exitCode = SpringApplication.exit(context, () -> 0);
        System.exit(exitCode);
    }

    private List<OutboxChange> latestRows() {
        return jdbcTemplate.query(
                """
                SELECT DISTINCT ON (job_id) job_id::text, op, payload::text
                FROM job_search_outbox
                ORDER BY job_id, created_at DESC, id DESC
                """,
                (rs, rowNum) -> {
                    String action = rs.getString("op");
                    if ("d".equals(action)) {
                        return new OutboxChange(rs.getString("job_id"), action, null);
                    }
                    String payload = rs.getString("payload");
                    try {
                        JsonNode document = payload == null ? null : objectMapper.readTree(payload);
                        return new OutboxChange(rs.getString("job_id"), action, document);
                    } catch (Exception exception) {
                        throw new IllegalStateException("Cannot read outbox payload for " + rs.getString("job_id"), exception);
                    }
                }
        ).stream().filter(change -> !change.delete()).toList();
    }

    private Set<String> indexesForAlias(String alias) throws Exception {
        if (!JobsIndex.exists(elasticsearchClient, alias)) {
            return Set.of();
        }
        GetAliasResponse response = elasticsearchClient.indices().getAlias(request -> request.name(alias));
        return response.result().keySet();
    }
}
