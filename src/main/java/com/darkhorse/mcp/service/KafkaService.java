package com.darkhorse.mcp.service;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class KafkaService {

    private final String bootstrapServers;

    public KafkaService(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    private AdminClient createAdminClient() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(properties);
    }

    public List<String> discoverTopics() throws Exception {
        try (AdminClient adminClient = createAdminClient()) {
            return new ArrayList<>(adminClient.listTopics().names().get());
        }
    }

    public Map<String, Object> analyzeTopicHealth(String topicName) throws Exception {
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
    }

    public String applyPartitionScaling(String topicName, int totalPartitions) throws Exception {
        try (AdminClient adminClient = createAdminClient()) {
            Map<String, NewPartitions> newPartitions = new HashMap<>();
            newPartitions.put(topicName, NewPartitions.increaseTo(totalPartitions));
            adminClient.createPartitions(newPartitions).all().get();
            return "Successfully scaled partitions for topic " + topicName + " to " + totalPartitions;
        }
    }

    public Map<String, String> auditTopicConfiguration(String topicName) throws Exception {
        try (AdminClient adminClient = createAdminClient()) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            Config config = adminClient.describeConfigs(Collections.singletonList(resource)).all().get().get(resource);

            Map<String, String> configMap = new HashMap<>();
            config.entries().forEach(entry -> {
                configMap.put(entry.name(), entry.value());
            });
            return configMap;
        }
    }

    public List<Map<String, Object>> analyzeConsumerLag(String groupId) throws Exception {
        try (AdminClient adminClient = createAdminClient()) {
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committedOffsets =
                    adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();

            if (committedOffsets.isEmpty()) {
                return Collections.emptyList();
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
    }

    public List<Map<String, Object>> inspectPartitionOffsets(String topicName) throws Exception {
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
    }

    public Map<String, Object> detectPartitionSkew(String topicName) throws Exception {
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
            result.put("is_highly_skewed", diff > 10000); // Arbitrary threshold

            return result;
        }
    }
}
