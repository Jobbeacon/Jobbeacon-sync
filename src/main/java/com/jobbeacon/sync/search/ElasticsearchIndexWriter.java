package com.jobbeacon.sync.search;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobbeacon.sync.config.SyncProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class ElasticsearchIndexWriter {

    private final ElasticsearchClient elasticsearchClient;
    private final ObjectMapper objectMapper;
    private final String index;

    public ElasticsearchIndexWriter(
            ElasticsearchClient elasticsearchClient,
            ObjectMapper objectMapper,
            SyncProperties properties
    ) {
        this.elasticsearchClient = elasticsearchClient;
        this.objectMapper = objectMapper;
        this.index = properties.getElasticsearch().getIndex();
    }

    public void apply(List<OutboxChange> changes) throws Exception {
        List<OutboxChange> latest = OutboxBatch.latestPerJob(changes);
        if (latest.isEmpty()) {
            return;
        }
        BulkResponse response = elasticsearchClient.bulk(bulk -> {
            for (OutboxChange change : latest) {
                if (change.delete()) {
                    bulk.operations(operation -> operation.delete(delete -> delete.index(index).id(change.jobId())));
                } else {
                    Map<String, Object> document = objectMapper.convertValue(change.payload(), Map.class);
                    bulk.operations(operation -> operation.index(indexOperation -> indexOperation
                            .index(index)
                            .id(change.jobId())
                            .document(document)));
                }
            }
            return bulk;
        });
        if (response.errors()) {
            throw new IllegalStateException("Elasticsearch rejected a job search bulk request");
        }
    }

    public void indexAll(String targetIndex, List<OutboxChange> changes) throws Exception {
        if (changes.isEmpty()) {
            return;
        }
        BulkResponse response = elasticsearchClient.bulk(bulk -> {
            for (OutboxChange change : changes) {
                if (change.delete() || change.payload() == null) {
                    continue;
                }
                Map<String, Object> document = objectMapper.convertValue(change.payload(), Map.class);
                bulk.operations(operation -> operation.index(indexOperation -> indexOperation
                        .index(targetIndex)
                        .id(change.jobId())
                        .document(document)));
            }
            return bulk;
        });
        if (response.errors()) {
            throw new IllegalStateException("Elasticsearch rejected the rebuild bulk request");
        }
    }
}
