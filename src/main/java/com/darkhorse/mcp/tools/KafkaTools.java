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

    @McpTool(description = "List all Kafka topics in the cluster")
    public Mono<List<String>> listKafkaTopics() {
        return Mono.fromCallable(() -> {
            try (AdminClient adminClient = createAdminClient()) {
                List<String> topics = new ArrayList<>(adminClient.listTopics().names().get());
                return topics;
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @McpTool(description = "Describe a specific Kafka topic to check how many partitions it has and replica information")
    public Mono<Map<String, Object>> describeKafkaTopic(String topicName) {
        return Mono.fromCallable(() -> {
            try (AdminClient adminClient = createAdminClient()) {
                TopicDescription description = adminClient.describeTopics(Collections.singletonList(topicName))
                        .allTopicNames().get().get(topicName);
                
                Map<String, Object> result = new HashMap<>();
                result.put("name", description.name());
                result.put("internal", description.isInternal());
                result.put("partitions_count", description.partitions().size());
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

    @McpTool(description = "Increase the number of partitions for a Kafka topic. NOTE: Total partitions must be greater than the current count.")
    public Mono<String> increaseTopicPartitions(String topicName, int totalPartitions) {
        return Mono.fromCallable(() -> {
            try (AdminClient adminClient = createAdminClient()) {
                Map<String, NewPartitions> newPartitions = new HashMap<>();
                newPartitions.put(topicName, NewPartitions.increaseTo(totalPartitions));
                adminClient.createPartitions(newPartitions).all().get();
                return "Successfully increased partitions for topic " + topicName + " to " + totalPartitions;
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @McpTool(description = "Get the configuration properties set for a Kafka topic (e.g. retention.ms, max.message.bytes)")
    public Mono<Map<String, String>> getTopicConfiguration(String topicName) {
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

    @McpTool(description = "Calculate consumer lag for a given consumer group across all its assigned partitions")
    public Mono<List<Map<String, Object>>> getConsumerGroupLag(String groupId) {
        return Mono.fromCallable(() -> {
            try (AdminClient adminClient = createAdminClient()) {
                // Get committed offsets for the group
                Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committedOffsets = 
                        adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
                
                if (committedOffsets.isEmpty()) {
                    return Collections.<Map<String, Object>>emptyList();
                }

                // Prepare request to get end offsets for those partitions
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

    @McpTool(description = "Get the current end offsets for all partitions of a topic to understand data volume and scalability")
    public Mono<List<Map<String, Object>>> getTopicOffsets(String topicName) {
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
}
