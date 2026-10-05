package com.jobbeacon.sync.search;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.jobbeacon.sync.config.SyncProperties;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class JobIndexInitializer {

    private final ElasticsearchClient elasticsearchClient;
    private final SyncProperties properties;

    public JobIndexInitializer(ElasticsearchClient elasticsearchClient, SyncProperties properties) {
        this.elasticsearchClient = elasticsearchClient;
        this.properties = properties;
    }

    public void ensureAlias() throws IOException {
        String alias = properties.getElasticsearch().getIndex();
        if (JobsIndex.exists(elasticsearchClient, alias)) {
            return;
        }
        String firstIndex = alias + "-v1";
        if (!JobsIndex.exists(elasticsearchClient, firstIndex)) {
            JobsIndex.create(elasticsearchClient, firstIndex);
        }
        elasticsearchClient.indices().putAlias(request -> request.index(firstIndex).name(alias).isWriteIndex(true));
    }
}
