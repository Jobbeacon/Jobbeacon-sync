package com.jobbeacon.sync.search;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.util.ObjectBuilder;

import java.io.IOException;

public final class JobsIndex {

    private JobsIndex() {
    }

    public static void create(ElasticsearchClient client, String index) throws IOException {
        client.indices().create(request -> request
                .index(index)
                .settings(JobsIndex::settings)
                .mappings(JobsIndex::mappings));
    }

    public static boolean exists(ElasticsearchClient client, String name) throws IOException {
        return client.indices().exists(ExistsRequest.of(request -> request.index(name))).value();
    }

    private static ObjectBuilder<IndexSettings> settings(IndexSettings.Builder settings) {
        return settings
                .numberOfShards("1")
                .numberOfReplicas("0")
                .analysis(analysis -> analysis.analyzer("vi_text", analyzer -> analyzer
                        .custom(custom -> custom.tokenizer("standard").filter("lowercase", "asciifolding"))));
    }

    private static co.elastic.clients.util.ObjectBuilder<co.elastic.clients.elasticsearch._types.mapping.TypeMapping> mappings(
            co.elastic.clients.elasticsearch._types.mapping.TypeMapping.Builder mappings
    ) {
        return mappings
                .properties("title", property -> property.text(text -> text.analyzer("vi_text")))
                .properties("description", property -> property.text(text -> text.analyzer("vi_text")))
                .properties("benefits", property -> property.text(text -> text.analyzer("vi_text")))
                .properties("company_id", property -> property.keyword(keyword -> keyword))
                .properties("company_name", property -> property.text(text -> text.analyzer("vi_text")
                        .fields("keyword", field -> field.keyword(keyword -> keyword))))
                .properties("company_logo_url", property -> property.keyword(keyword -> keyword.index(false)))
                .properties("skills", property -> property.object(object -> object
                        .properties("id", field -> field.keyword(keyword -> keyword))
                        .properties("name", field -> field.text(text -> text.analyzer("vi_text")))))
                .properties("categories", property -> property.object(object -> object
                        .properties("id", field -> field.keyword(keyword -> keyword))
                        .properties("name", field -> field.text(text -> text.analyzer("vi_text")))))
                .properties("locations", property -> property.text(text -> text.analyzer("vi_text")))
                .properties("province_ids", property -> property.keyword(keyword -> keyword))
                .properties("province_names", property -> property.text(text -> text.analyzer("vi_text")))
                .properties("employment_type", property -> property.keyword(keyword -> keyword))
                .properties("experience_months", property -> property.integer(number -> number))
                .properties("min_salary", property -> property.long_(number -> number))
                .properties("max_salary", property -> property.long_(number -> number))
                .properties("currency", property -> property.keyword(keyword -> keyword))
                .properties("salary_period", property -> property.keyword(keyword -> keyword))
                .properties("posted_date", property -> property.date(date -> date))
                .properties("expired_date", property -> property.date(date -> date))
                .properties("sources", property -> property.object(object -> object
                        .properties("domain", field -> field.keyword(keyword -> keyword))
                        .properties("job_url", field -> field.keyword(keyword -> keyword.index(false)))));
    }
}
