package com.darkhorse.mcp.tools;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class KafkaTools {

    private final String bootstrapServers;

    public KafkaTools(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    private AdminClient createAdminClient() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(properties);
    }

    @McpTool(description = "[Risk: LOW, Read-Only: true] Discover active Kafka topics in the cluster to begin topology investigation.")
    public Mono<List<String>> discoverTopics() {
        return Mono.fromCallable(() -> {
            try (AdminClient adminClient = createAdminClient()) {
                List<String> topics = new ArrayList<>(adminClient.listTopics().names().get());
                return topics;
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @McpTool(description = "[Risk: LOW, Read-Only: true] Analyze topic health, verifying partition count, leader assignments, and under-replicated partitions (ISR discrepancies).")
    public Mono<Map<String, Object>> analyzeTopicHealth(String topicName) {
        return Mono.fromCallable(() -> {
            try (AdminClient adminClient = createAdminClient()) {
                TopicDescription description = adminClient.describeTopics(Collections.singletonList(topicName))
                        .allTopicNames().get().get(topicName);
                
                Map<String, Object> result = new HashMap<>();
                result.put("name", description.name());
                result.put("internal", description.isInternal());
                result.put("partitions_count", description.partitions().size());
                
                int underReplicatedCount = 0;
                for (org.apache.kafka.common.TopicPartitionInfo p : description.partitions()) {
                    if (p.isr().size() < p.replicas().size()) {
                        underReplicatedCount++;
                    }
                }
                result.put("under_replicated_partitions", underReplicatedCount);

                result.put("partitions", description.partitions().stream().map(p -> {
                    Map<String, Object> partMap = new HashMap<>();
                    partMap.put("partition", p.partition());
                    partMap.put("leader", p.leader() != null ? p.leader().id() : null);
                    partMap.put("replicas", p.replicas().stream().map(n -> n.id()).collect(Collectors.toList()));
                    partMap.put("isr", p.isr().stream().map(n -> n.id()).collect(Collectors.toList()));
                    return partMap;
                }).collect(Collectors.toList()));
                return result;
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @McpTool(description = "[Risk: MEDIUM, Read-Only: false] Apply an approved operational migration to dynamically scale a topic's partition count. Required when consumer concurrency bottlenecks are identified.",
             annotations = @McpTool.McpAnnotations(destructiveHint = true))
    public Mono<String> applyPartitionScaling(String topicName, int totalPartitions) {
        return Mono.fromCallable(() -> {
            try (AdminClient adminClient = createAdminClient()) {
                Map<String, NewPartitions> newPartitions = new HashMap<>();
                newPartitions.put(topicName, NewPartitions.increaseTo(totalPartitions));
                adminClient.createPartitions(newPartitions).all().get();
                return "Successfully scaled partitions for topic " + topicName + " to " + totalPartitions;
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @McpTool(description = "[Risk: LOW, Read-Only: true] Audit topic configuration for retention policies, compression, and segment sizes to diagnose capacity or throughput bottlenecks.")
    public Mono<Map<String, String>> auditTopicConfiguration(String topicName) {
        return Mono.fromCallable(() -> {
            try (AdminClient adminClient = createAdminClient()) {
                ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
                Config config = adminClient.describeConfigs(Collections.singletonList(resource)).all().get().get(resource);
                
                Map<String, String> configMap = new HashMap<>();
                config.entries().forEach(entry -> {
                    configMap.put(entry.name(), entry.value());
                });
                return configMap;
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @McpTool(description = "[Risk: LOW, Read-Only: true] Analyze consumer lag to determine if a consumer group is failing to keep up with ingestion rates. Use this to trigger partition scaling workflows.")
    public Mono<List<Map<String, Object>>> analyzeConsumerLag(String groupId) {
        return Mono.fromCallable(() -> {
            try (AdminClient adminClient = createAdminClient()) {
                Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committedOffsets = 
                        adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
                
                if (committedOffsets.isEmpty()) {
                    return Collections.<Map<String, Object>>emptyList();
                }

                Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
                for (TopicPartition tp : committedOffsets.keySet()) {
                    requestLatestOffsets.put(tp, OffsetSpec.latest());
                }

                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = 
                        adminClient.listOffsets(requestLatestOffsets).all().get();

                List<Map<String, Object>> lagList = new ArrayList<>();
                for (Map.Entry<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> entry : committedOffsets.entrySet()) {
                    TopicPartition tp = entry.getKey();
                    long committed = entry.getValue().offset();
                    long latest = latestOffsets.get(tp).offset();
                    long lag = latest - committed;

                    Map<String, Object> lagInfo = new HashMap<>();
                    lagInfo.put("topic", tp.topic());
                    lagInfo.put("partition", tp.partition());
                    lagInfo.put("committed_offset", committed);
                    lagInfo.put("latest_offset", latest);
                    lagInfo.put("lag", lag);
                    lagList.add(lagInfo);
                }
                return lagList;
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @McpTool(description = "[Risk: LOW, Read-Only: true] Inspect partition offsets to map throughput distribution and detect dead partitions.")
    public Mono<List<Map<String, Object>>> inspectPartitionOffsets(String topicName) {
        return Mono.fromCallable(() -> {
            try (AdminClient adminClient = createAdminClient()) {
                TopicDescription description = adminClient.describeTopics(Collections.singletonList(topicName))
                        .allTopicNames().get().get(topicName);

                Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
                for (org.apache.kafka.common.TopicPartitionInfo p : description.partitions()) {
                    requestLatestOffsets.put(new TopicPartition(topicName, p.partition()), OffsetSpec.latest());
                }

                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = 
                        adminClient.listOffsets(requestLatestOffsets).all().get();

                List<Map<String, Object>> offsetList = new ArrayList<>();
                for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry : latestOffsets.entrySet()) {
                    Map<String, Object> info = new HashMap<>();
                    info.put("partition", entry.getKey().partition());
                    info.put("end_offset", entry.getValue().offset());
                    offsetList.add(info);
                }
                return offsetList;
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @McpTool(description = "[Risk: LOW, Read-Only: true] Detect partition skew to diagnose hot-spotting or poor partitioning keys. Calculates the variance between the highest and lowest partition offsets.")
    public Mono<Map<String, Object>> detectPartitionSkew(String topicName) {
        return Mono.fromCallable(() -> {
            try (AdminClient adminClient = createAdminClient()) {
                TopicDescription description = adminClient.describeTopics(Collections.singletonList(topicName))
                        .allTopicNames().get().get(topicName);

                Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
                for (org.apache.kafka.common.TopicPartitionInfo p : description.partitions()) {
                    requestLatestOffsets.put(new TopicPartition(topicName, p.partition()), OffsetSpec.latest());
                }

                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = 
                        adminClient.listOffsets(requestLatestOffsets).all().get();

                long maxOffset = -1;
                long minOffset = Long.MAX_VALUE;
                for (ListOffsetsResult.ListOffsetsResultInfo info : latestOffsets.values()) {
                    if (info.offset() > maxOffset) maxOffset = info.offset();
                    if (info.offset() < minOffset) minOffset = info.offset();
                }

                long diff = maxOffset - (minOffset == Long.MAX_VALUE ? 0 : minOffset);
                
                Map<String, Object> result = new HashMap<>();
                result.put("topic", topicName);
                result.put("max_offset", maxOffset);
                result.put("min_offset", minOffset == Long.MAX_VALUE ? 0 : minOffset);
                result.put("skew_variance", diff);
                result.put("is_highly_skewed", diff > 10000); // Arbitrary threshold for flagging

                return result;
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }
}
