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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class RemoteLogSegmentLifecycleTest {
    private static final Logger log = LoggerFactory.getLogger(RemoteLogSegmentLifecycleTest.class);

    private static final int SEG_SIZE = 1024 * 1024;
    private static final int BROKER_ID_0 = 0;
    private static final int BROKER_ID_1 = 1;

    private final TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
    private final Time time = new MockTime(1);

    @ParameterizedTest(name = "testCacheSegmentWithCopySegmentStartedState = {0}")
    @MethodSource("remoteLogSegmentLifecycleManagers")
    public void testCacheSegmentWithCopySegmentStartedState(RemoteLogSegmentLifecycleManager remoteLogSegmentLifecycleManager) throws Exception {

        try {
            remoteLogSegmentLifecycleManager.initialize(topicIdPartition);

                // Create a segment with state COPY_SEGMENT_STARTED, and check for searching that segment and listing the
            // segments.
            RemoteLogSegmentId segmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
            RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(segmentId, 0L, 50L, -1L, BROKER_ID_0,
                                                                                    time.milliseconds(), SEG_SIZE, Collections.singletonMap(0, 0L));
            remoteLogSegmentLifecycleManager.addRemoteLogSegmentMetadata(segmentMetadata);

            // This segment should not be available as the state is not reached to COPY_SEGMENT_FINISHED.
            Optional<RemoteLogSegmentMetadata> segMetadataForOffset0Epoch0 = remoteLogSegmentLifecycleManager.remoteLogSegmentMetadata(0, 0);
            assertFalse(segMetadataForOffset0Epoch0.isPresent());

            // cache.listRemoteLogSegments APIs should contain the above segment.
            RemoteLogMetadataCacheTest.checkListSegments(remoteLogSegmentLifecycleManager, 0, segmentMetadata);
        } finally {
            Utils.closeQuietly(remoteLogSegmentLifecycleManager, "RemoteLogSegmentLifecycleManager");
        }
    }

    @ParameterizedTest(name = "testCacheSegmentWithCopySegmentFinishedState = {0}")
    @MethodSource("remoteLogSegmentLifecycleManagers")
    public void testCacheSegmentWithCopySegmentFinishedState(RemoteLogSegmentLifecycleManager remoteLogSegmentLifecycleManager) throws Exception {
        try {
            remoteLogSegmentLifecycleManager.initialize(topicIdPartition);

            // Create a segment and move it to state COPY_SEGMENT_FINISHED. and check for searching that segment and
            // listing the segments.
            RemoteLogSegmentMetadata segmentMetadata = createSegmentUpdateWithState(remoteLogSegmentLifecycleManager,
                                                                                    Collections.singletonMap(0, 101L),
                                                                                    101L, 200L, RemoteLogSegmentState.COPY_SEGMENT_FINISHED);

            // Search should return the above segment.
            Optional<RemoteLogSegmentMetadata> segMetadataForOffset150 = remoteLogSegmentLifecycleManager.remoteLogSegmentMetadata(0, 150);
            assertEquals(Optional.of(segmentMetadata), segMetadataForOffset150);

            // cache.listRemoteLogSegments should contain the above segments.
            RemoteLogMetadataCacheTest.checkListSegments(remoteLogSegmentLifecycleManager, 0, segmentMetadata);
        } finally {
            Utils.closeQuietly(remoteLogSegmentLifecycleManager, "RemoteLogSegmentLifecycleManager");
        }
    }

    @ParameterizedTest(name = "testCacheSegmentWithDeleteSegmentStartedState = {0}")
    @MethodSource("remoteLogSegmentLifecycleManagers")
    public void testCacheSegmentWithDeleteSegmentStartedState(RemoteLogSegmentLifecycleManager remoteLogSegmentLifecycleManager) throws Exception {
        try {
            remoteLogSegmentLifecycleManager.initialize(topicIdPartition);

            // Create a segment and move it to state DELETE_SEGMENT_STARTED, and check for searching that segment and
            // listing the segments.
            RemoteLogSegmentMetadata segmentMetadata = createSegmentUpdateWithState(remoteLogSegmentLifecycleManager,
                                                                                    Collections.singletonMap(0, 201L),
                                                                                    201L, 300L, RemoteLogSegmentState.DELETE_SEGMENT_STARTED);

            // Search should not return the above segment as their leader epoch state is cleared.
            Optional<RemoteLogSegmentMetadata> segmentMetadataForOffset250Epoch0 = remoteLogSegmentLifecycleManager.remoteLogSegmentMetadata(0, 250);
            assertFalse(segmentMetadataForOffset250Epoch0.isPresent());

            RemoteLogMetadataCacheTest.checkListSegments(remoteLogSegmentLifecycleManager, 0, segmentMetadata);
        } finally {
            Utils.closeQuietly(remoteLogSegmentLifecycleManager, "RemoteLogSegmentLifecycleManager");
        }
    }

    @ParameterizedTest(name = "testCacheSegmentsWithDeleteSegmentFinishedState = {0}")
    @MethodSource("remoteLogSegmentLifecycleManagers")
    public void testCacheSegmentsWithDeleteSegmentFinishedState(RemoteLogSegmentLifecycleManager remoteLogSegmentLifecycleManager) throws Exception {
        try {
            remoteLogSegmentLifecycleManager.initialize(topicIdPartition);

            // Create a segment and move it to state DELETE_SEGMENT_FINISHED, and check for searching that segment and
            // listing the segments.
            RemoteLogSegmentMetadata segmentMetadata = createSegmentUpdateWithState(remoteLogSegmentLifecycleManager,
                                                                                    Collections.singletonMap(0, 301L),
                                                                                    301L, 400L, RemoteLogSegmentState.DELETE_SEGMENT_STARTED);

            // Search should not return the above segment as their leader epoch state is cleared.
            assertFalse(remoteLogSegmentLifecycleManager.remoteLogSegmentMetadata(0, 350).isPresent());

            RemoteLogSegmentMetadataUpdate segmentMetadataUpdate = new RemoteLogSegmentMetadataUpdate(segmentMetadata.remoteLogSegmentId(),
                                                                                                      time.milliseconds(),
                                                                                                      RemoteLogSegmentState.DELETE_SEGMENT_FINISHED,
                                                                                                      BROKER_ID_1);
            remoteLogSegmentLifecycleManager.updateRemoteLogSegmentMetadata(segmentMetadataUpdate);

            // listRemoteLogSegments(0) and listRemoteLogSegments() should not contain the above segment.
            assertFalse(remoteLogSegmentLifecycleManager.listRemoteLogSegments(0).hasNext());
            assertFalse(remoteLogSegmentLifecycleManager.listAllRemoteLogSegments().hasNext());
        } finally {
            Utils.closeQuietly(remoteLogSegmentLifecycleManager, "RemoteLogSegmentLifecycleManager");
        }
    }

    private static Collection<Arguments> remoteLogSegmentLifecycleManagers() {
        return Arrays.asList(
                Arguments.of(new RemoteLogMetadataCacheWrapper()),
                Arguments.of(new TopicBasedRemoteLogMetadataManagerWrapper())
        );
    }

    private RemoteLogSegmentMetadata createSegmentUpdateWithState(RemoteLogSegmentLifecycleManager remoteLogSegmentLifecycleManager,
                                                                  Map<Integer, Long> segmentLeaderEpochs,
                                                                  long startOffset,
                                                                  long endOffset,
                                                                  RemoteLogSegmentState state)
            throws RemoteStorageException {
        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(segmentId, startOffset, endOffset, -1L, BROKER_ID_0,
                time.milliseconds(), SEG_SIZE, segmentLeaderEpochs);
        remoteLogSegmentLifecycleManager.addRemoteLogSegmentMetadata(segmentMetadata);

        RemoteLogSegmentMetadataUpdate segMetadataUpdate = new RemoteLogSegmentMetadataUpdate(segmentId, time.milliseconds(), state, BROKER_ID_1);
        remoteLogSegmentLifecycleManager.updateRemoteLogSegmentMetadata(segMetadataUpdate);

        return segmentMetadata.createWithUpdates(segMetadataUpdate);
    }

    /**
     * This is a wrapper with {@link TopicBasedRemoteLogMetadataManager} implementing {@link RemoteLogSegmentLifecycleManager}.
     * This is passed to {@link RemoteLogMetadataCacheTest#testSegmentsLifeCycleInCache(RemoteLogSegmentLifecycleManager)} to test
     * {@code RemoteLogMetadataCache} for several lifecycle operations.
     * <p>
     * This starts a Kafka cluster with {@link #initialize(TopicIdPartition)} with {@link #brokerCount()} no of servers. It also
     * creates the remote log metadata topic required for {@code TopicBasedRemoteLogMetadataManager}. This cluster will
     * be stopped by invoking {@link #close()}.
     */
    static class TopicBasedRemoteLogMetadataManagerWrapper extends TopicBasedRemoteLogMetadataManagerHarness implements RemoteLogSegmentLifecycleManager {

        private TopicIdPartition topicIdPartition;

        @Override
        public synchronized void initialize(TopicIdPartition topicIdPartition) {
            this.topicIdPartition = topicIdPartition;
            super.initialize(Collections.singleton(topicIdPartition), true);
        }

        @Override
        public void addRemoteLogSegmentMetadata(RemoteLogSegmentMetadata segmentMetadata) throws RemoteStorageException {
            remoteLogMetadataManager().addRemoteLogSegmentMetadata(segmentMetadata);
        }

        @Override
        public void updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate segmentMetadataUpdate) throws RemoteStorageException {
            remoteLogMetadataManager().updateRemoteLogSegmentMetadata(segmentMetadataUpdate);
        }

        @Override
        public Optional<Long> highestOffsetForEpoch(int leaderEpoch) throws RemoteStorageException {
            return remoteLogMetadataManager().highestOffsetForEpoch(topicIdPartition, leaderEpoch);
        }

        @Override
        public Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(int leaderEpoch,
                                                                           long offset) throws RemoteStorageException {
            return remoteLogMetadataManager().remoteLogSegmentMetadata(topicIdPartition, leaderEpoch, offset);
        }

        @Override
        public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(int leaderEpoch) throws RemoteStorageException {
            return remoteLogMetadataManager().listRemoteLogSegments(topicIdPartition, leaderEpoch);
        }

        @Override
        public Iterator<RemoteLogSegmentMetadata> listAllRemoteLogSegments() throws RemoteStorageException {
            return remoteLogMetadataManager().listRemoteLogSegments(topicIdPartition);
        }

        @Override
        public void close() throws IOException {
            super.close();
        }

        @Override
        public int brokerCount() {
            return super.brokerCount();
        }
    }

    /**
     * This is a wrapper with {@link RemoteLogMetadataCache} implementing {@link RemoteLogSegmentLifecycleManager}.
     * This is passed to {@link RemoteLogMetadataCacheTest#testSegmentsLifeCycleInCache(RemoteLogSegmentLifecycleManager)} to test
     * {@code RemoteLogMetadataCache} for several lifecycle operations.
     */
    static class RemoteLogMetadataCacheWrapper implements RemoteLogSegmentLifecycleManager {

        private final RemoteLogMetadataCache metadataCache = new RemoteLogMetadataCache();

        @Override
        public void updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate segmentMetadataUpdate) throws RemoteStorageException {
            metadataCache.updateRemoteLogSegmentMetadata(segmentMetadataUpdate);
        }

        @Override
        public Optional<Long> highestOffsetForEpoch(int epoch) throws RemoteStorageException {
            return metadataCache.highestOffsetForEpoch(epoch);
        }

        @Override
        public Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(int leaderEpoch,
                                                                           long offset) throws RemoteStorageException {
            return metadataCache.remoteLogSegmentMetadata(leaderEpoch, offset);
        }

        @Override
        public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(int leaderEpoch) throws RemoteResourceNotFoundException {
            return metadataCache.listRemoteLogSegments(leaderEpoch);
        }

        @Override
        public Iterator<RemoteLogSegmentMetadata> listAllRemoteLogSegments() {
            return metadataCache.listAllRemoteLogSegments();
        }

        @Override
        public void addRemoteLogSegmentMetadata(RemoteLogSegmentMetadata segmentMetadata) throws RemoteStorageException {
            metadataCache.addCopyInProgressSegment(segmentMetadata);
        }
    }

    private static class EpochOffset {
        final int epoch;
        final long offset;

        EpochOffset(int epoch,
                    long offset) {
            this.epoch = epoch;
            this.offset = offset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            EpochOffset that = (EpochOffset) o;
            return epoch == that.epoch && offset == that.offset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(epoch, offset);
        }

        @Override
        public String toString() {
            return "EpochOffset{" +
                    "epoch=" + epoch +
                    ", offset=" + offset +
                    '}';
        }
    }
}
