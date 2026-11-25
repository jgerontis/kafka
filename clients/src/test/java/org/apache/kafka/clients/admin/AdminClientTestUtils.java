/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.admin;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.*;
import org.apache.kafka.clients.admin.CreateTopicsResult.TopicMetadataAndConfig;
import org.apache.kafka.clients.admin.internals.AdminMetadataManager;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.metrics.*;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import static org.apache.kafka.common.utils.Utils.closeQuietly;

public class AdminClientTestUtils {

    /**
     * Helper to create a ListPartitionReassignmentsResult instance for a given Throwable.
     * ListPartitionReassignmentsResult's constructor is only accessible from within the
     * admin package.
     */
    public static ListPartitionReassignmentsResult listPartitionReassignmentsResult(Throwable t) {
        KafkaFutureImpl<Map<TopicPartition, PartitionReassignment>> future = new KafkaFutureImpl<>();
        future.completeExceptionally(t);
        return new ListPartitionReassignmentsResult(future);
    }

    /**
     * Helper to create a CreateTopicsResult instance for a given Throwable.
     * CreateTopicsResult's constructor is only accessible from within the
     * admin package.
     */
    public static CreateTopicsResult createTopicsResult(String topic, Throwable t) {
        KafkaFutureImpl<TopicMetadataAndConfig> future = new KafkaFutureImpl<>();
        future.completeExceptionally(t);
        return new CreateTopicsResult(Collections.singletonMap(topic, future));
    }

    /**
     * Helper to create a DeleteTopicsResult instance for a given Throwable.
     * DeleteTopicsResult's constructor is only accessible from within the
     * admin package.
     */
    public static DeleteTopicsResult deleteTopicsResult(String topic, Throwable t) {
        KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        future.completeExceptionally(t);
        return DeleteTopicsResult.ofTopicNames(Collections.singletonMap(topic, future));
    }

    /**
     * Helper to create a ListTopicsResult instance for a given topic.
     * ListTopicsResult's constructor is only accessible from within the
     * admin package.
     */
    public static ListTopicsResult listTopicsResult(String topic) {
        KafkaFutureImpl<Map<String, TopicListing>> future = new KafkaFutureImpl<>();
        future.complete(Collections.singletonMap(topic, new TopicListing(topic, false)));
        return new ListTopicsResult(future);
    }

    /**
     * Helper to create a CreatePartitionsResult instance for a given Throwable.
     * CreatePartitionsResult's constructor is only accessible from within the
     * admin package.
     */
    public static CreatePartitionsResult createPartitionsResult(String topic, Throwable t) {
        KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        future.completeExceptionally(t);
        return new CreatePartitionsResult(Collections.singletonMap(topic, future));
    }

    /**
     * Helper to create a DescribeTopicsResult instance for a given topic.
     * DescribeTopicsResult's constructor is only accessible from within the
     * admin package.
     */
    public static DescribeTopicsResult describeTopicsResult(String topic, TopicDescription description) {
        KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
        future.complete(description);
        return new DescribeTopicsResult(Collections.singletonMap(topic, future));
    }

    public static DescribeTopicsResult describeTopicsResult(Map<String, TopicDescription> topicDescriptions) {
        return new DescribeTopicsResult(topicDescriptions.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> KafkaFuture.completedFuture(e.getValue()))));
    }

    public static ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult(Map<TopicPartition, OffsetAndMetadata> offsets) {
        return new ListConsumerGroupOffsetsResult(KafkaFuture.completedFuture(offsets));
    }

    /**
     * Helper to create a KafkaAdminClient with a custom HostResolver accessible to tests outside this package.
     */
    public static Admin create(Map<String, Object> conf, HostResolver hostResolver) {
        AdminClientConfig config = new AdminClientConfig(conf, true);
        Metrics metrics = null;
        NetworkClient networkClient = null;
        Time time = Time.SYSTEM;
        String clientId = KafkaAdminClient.generateClientId(config);
        ChannelBuilder channelBuilder = null;
        Selector selector = null;
        ApiVersions apiVersions = new ApiVersions();
        LogContext logContext = KafkaAdminClient.createLogContext(clientId);
        LeastLoadedNodeAlgorithm leastLoadedNodeAlgorithm = LeastLoadedNodeAlgorithm.valueOf(
            config.getString(AdminClientConfig.LEAST_LOADED_NODE_ALGORITHM_CONFIG)
        );

        try {
            // Since we only request node information, it's safe to pass true for allowAutoTopicCreation (and it
            // simplifies communication with older brokers)
            AdminMetadataManager metadataManager = new AdminMetadataManager(logContext,
                config.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG),
                config.getLong(AdminClientConfig.METADATA_MAX_AGE_CONFIG));
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
                    config.getList(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG),
                    config.getString(AdminClientConfig.CLIENT_DNS_LOOKUP_CONFIG));
            metadataManager.update(Cluster.bootstrap(addresses), time.milliseconds());
            List<MetricsReporter> reporters = config.getConfiguredInstances(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG,
                MetricsReporter.class,
                Collections.singletonMap(AdminClientConfig.CLIENT_ID_CONFIG, clientId));
            Map<String, String> metricTags = Collections.singletonMap("client-id", clientId);
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt(AdminClientConfig.METRICS_NUM_SAMPLES_CONFIG))
                .timeWindow(config.getLong(AdminClientConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                .recordLevel(Sensor.RecordingLevel.forName(config.getString(AdminClientConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                .tags(metricTags);
            JmxReporter jmxReporter = new JmxReporter();
            jmxReporter.configure(config.originals());
            reporters.add(jmxReporter);
            MetricsContext metricsContext = new KafkaMetricsContext(KafkaAdminClient.JMX_PREFIX,
                    config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX));
            metrics = new Metrics(metricConfig, reporters, time, metricsContext);
            metrics.setReplaceOnDuplicateMetric(config.getBoolean(AdminClientConfig.METRICS_REPLACE_ON_DUPLICATE_CONFIG));
            String metricGrpPrefix = "admin-client";
            channelBuilder = ClientUtils.createChannelBuilder(config, time, logContext);
            selector = new Selector(config.getLong(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                    metrics, time, metricGrpPrefix, channelBuilder, logContext);
            networkClient = new NetworkClient(
                metadataManager.updater(),
                null,
                selector,
                clientId,
                1,
                config.getLong(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG),
                config.getLong(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                config.getInt(AdminClientConfig.SEND_BUFFER_CONFIG),
                config.getInt(AdminClientConfig.RECEIVE_BUFFER_CONFIG),
                (int) TimeUnit.HOURS.toMillis(1),
                config.getLong(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
                config.getLong(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
                time,
                true,
                apiVersions,
                null,
                logContext,
                (hostResolver == null) ? new DefaultHostResolver() : hostResolver,
                null,
                leastLoadedNodeAlgorithm,
                Collections.emptyList());
            return new KafkaAdminClient(config, clientId, time, metadataManager, metrics, networkClient,
                    null, logContext);
        } catch (Throwable exc) {
            closeQuietly(metrics, "Metrics");
            closeQuietly(networkClient, "NetworkClient");
            closeQuietly(selector, "Selector");
            closeQuietly(channelBuilder, "ChannelBuilder");
            throw new KafkaException("Failed to create new KafkaAdminClient", exc);
        }
    }
}
