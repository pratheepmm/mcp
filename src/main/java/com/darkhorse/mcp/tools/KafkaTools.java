package com.darkhorse.mcp.tools;

import com.darkhorse.mcp.service.KafkaService;
import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
public class KafkaTools {

    private final KafkaService kafkaService;

    public KafkaTools(KafkaService kafkaService) {
        this.kafkaService = kafkaService;
    }

    public enum KafkaDiagnosticAction {
        DISCOVER_TOPICS,
        ANALYZE_TOPIC_HEALTH,
        AUDIT_TOPIC_CONFIG,
        ANALYZE_CONSUMER_LAG,
        INSPECT_PARTITION_OFFSETS,
        DETECT_PARTITION_SKEW
    }

    public record KafkaDiagnosticRequest(
            KafkaDiagnosticAction action,
            String topicName,
            String groupId
    ) {}

    @McpTool(description = "[Risk: LOW, Read-Only: true] Unified Kafka diagnostic tool. Discovers topics, analyzes health, audits configurations, measures consumer lag, and detects partition skew.")
    public Mono<Object> kafkaDiagnostics(KafkaDiagnosticRequest request) {
        return Mono.<Object>fromCallable(() -> {
            return switch (request.action()) {
                case DISCOVER_TOPICS -> kafkaService.discoverTopics();
                case ANALYZE_TOPIC_HEALTH -> kafkaService.analyzeTopicHealth(request.topicName());
                case AUDIT_TOPIC_CONFIG -> kafkaService.auditTopicConfiguration(request.topicName());
                case ANALYZE_CONSUMER_LAG -> kafkaService.analyzeConsumerLag(request.groupId());
                case INSPECT_PARTITION_OFFSETS -> kafkaService.inspectPartitionOffsets(request.topicName());
                case DETECT_PARTITION_SKEW -> kafkaService.detectPartitionSkew(request.topicName());
            };
        }).subscribeOn(Schedulers.boundedElastic());
    }

    public enum KafkaMigrationAction {
        APPLY_PARTITION_SCALING
    }

    public record KafkaMigrationRequest(
            KafkaMigrationAction action,
            String topicName,
            Integer totalPartitions
    ) {}

    @McpTool(description = "[Risk: MEDIUM, Read-Only: false] Unified Kafka migration tool. Apply an approved operational migration to dynamically scale a topic's partition count.",
             annotations = @McpTool.McpAnnotations(destructiveHint = true))
    public Mono<String> kafkaMigration(KafkaMigrationRequest request) {
        return Mono.fromCallable(() -> {
            return switch (request.action()) {
                case APPLY_PARTITION_SCALING -> kafkaService.applyPartitionScaling(request.topicName(), request.totalPartitions());
            };
        }).subscribeOn(Schedulers.boundedElastic());
    }
}
