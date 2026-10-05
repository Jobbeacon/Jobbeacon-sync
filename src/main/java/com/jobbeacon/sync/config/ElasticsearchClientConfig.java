package com.jobbeacon.sync.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchClientConfig {

    @Bean(destroyMethod = "close")
    RestClient elasticsearchRestClient(SyncProperties properties) {
        return RestClient.builder(HttpHost.create(properties.getElasticsearch().getUrl()))
                .setRequestConfigCallback(config -> config.setConnectTimeout(5_000).setSocketTimeout(120_000))
                .build();
    }

    @Bean
    ElasticsearchClient elasticsearchClient(RestClient elasticsearchRestClient) {
        return new ElasticsearchClient(new RestClientTransport(elasticsearchRestClient, new JacksonJsonpMapper()));
    }
}
