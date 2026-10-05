package com.jobbeacon.sync.search;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OutboxBatch {

    private OutboxBatch() {
    }

    public static List<OutboxChange> latestPerJob(List<OutboxChange> changes) {
        Map<String, OutboxChange> latest = new LinkedHashMap<>();
        for (OutboxChange change : changes) {
            latest.put(change.jobId(), change);
        }
        return List.copyOf(latest.values());
    }
}
