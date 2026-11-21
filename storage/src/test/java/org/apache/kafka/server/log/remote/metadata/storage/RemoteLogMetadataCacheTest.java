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
import org.apache.kafka.server.log.remote.storage.*;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Stream;

public class RemoteLogMetadataCacheTest {

    private static final TopicIdPartition TP0 = new TopicIdPartition(Uuid.randomUuid(),
            new TopicPartition("foo", 0));
    private static final int SEG_SIZE = 1024 * 1024;
    private static final int BROKER_ID_0 = 0;
    private static final int BROKER_ID_1 = 1;

    private final Time time = new MockTime(1);

    @Test
    public void testAPIsWithInvalidArgs() {
        RemoteLogMetadataCache cache = new RemoteLogMetadataCache();

        Assertions.assertThrows(NullPointerException.class, () -> cache.addCopyInProgressSegment(null));
        Assertions.assertThrows(NullPointerException.class, () -> cache.updateRemoteLogSegmentMetadata(null));

        // Check for invalid state updates to addCopyInProgressSegment method.
        for (RemoteLogSegmentState state : RemoteLogSegmentState.values()) {
            if (state != RemoteLogSegmentState.COPY_SEGMENT_STARTED) {
                RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(
                        new RemoteLogSegmentId(TP0, Uuid.randomUuid()), 0, 100L,
                        -1L, BROKER_ID_0, time.milliseconds(), SEG_SIZE, Collections.singletonMap(0, 0L));
                RemoteLogSegmentMetadata updatedMetadata = segmentMetadata
                        .createWithUpdates(new RemoteLogSegmentMetadataUpdate(segmentMetadata.remoteLogSegmentId(),
                                time.milliseconds(), state, BROKER_ID_1));
                Assertions.assertThrows(IllegalArgumentException.class, () ->
                        cache.addCopyInProgressSegment(updatedMetadata));
            }
        }

        // Check for updating non existing segment-id.
        Assertions.assertThrows(RemoteResourceNotFoundException.class, () -> {
            RemoteLogSegmentId nonExistingId = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
            cache.updateRemoteLogSegmentMetadata(new RemoteLogSegmentMetadataUpdate(nonExistingId,
                    time.milliseconds(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED, BROKER_ID_1));
        });

        // Check for invalid state transition.
        Assertions.assertThrows(IllegalStateException.class, () -> {
            RemoteLogSegmentMetadata segmentMetadata = createSegmentUpdateWithState(cache, Collections.singletonMap(0, 0L), 0,
                    100, RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
            cache.updateRemoteLogSegmentMetadata(new RemoteLogSegmentMetadataUpdate(segmentMetadata.remoteLogSegmentId(),
                    time.milliseconds(), RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, BROKER_ID_1));
        });
    }

    private RemoteLogSegmentMetadata createSegmentUpdateWithState(RemoteLogMetadataCache cache,
                                                                  Map<Integer, Long> segmentLeaderEpochs,
                                                                  long startOffset,
                                                                  long endOffset,
                                                                  RemoteLogSegmentState state)
            throws RemoteResourceNotFoundException {
        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(segmentId, startOffset, endOffset, -1L,
                                                                                BROKER_ID_0, time.milliseconds(), SEG_SIZE, segmentLeaderEpochs);
        cache.addCopyInProgressSegment(segmentMetadata);

        RemoteLogSegmentMetadataUpdate segMetadataUpdate = new RemoteLogSegmentMetadataUpdate(segmentId,
                                                                                              time.milliseconds(), state, BROKER_ID_1);
        cache.updateRemoteLogSegmentMetadata(segMetadataUpdate);

        return segmentMetadata.createWithUpdates(segMetadataUpdate);
    }

    private void checkListSegments(RemoteLogSegmentLifecycleManager remoteLogSegmentLifecycleManager,
                                   int leaderEpoch,
                                   RemoteLogSegmentMetadata expectedSegment)
            throws RemoteStorageException {
        // cache.listRemoteLogSegments(leaderEpoch) should contain the above segment.
        Iterator<RemoteLogSegmentMetadata> segmentsIter = remoteLogSegmentLifecycleManager.listRemoteLogSegments(leaderEpoch);
        assertTrue(segmentsIter.hasNext() && Objects.equals(segmentsIter.next(), expectedSegment));

        // cache.listAllRemoteLogSegments() should contain the above segment.
        Iterator<RemoteLogSegmentMetadata> allSegmentsIter = remoteLogSegmentLifecycleManager.listAllRemoteLogSegments();
        assertTrue(allSegmentsIter.hasNext() && Objects.equals(allSegmentsIter.next(), expectedSegment));
    }

    @ParameterizedTest(name = "testCacheListSegments = {0}")
    @MethodSource("remoteLogSegmentLifecycleManagers")
    public void testCacheListSegments(RemoteLogSegmentLifecycleManager remoteLogSegmentLifecycleManager) throws Exception {
        try {
            remoteLogSegmentLifecycleManager.initialize(topicIdPartition);

            // Create a few segments and add them to the cache.
            RemoteLogSegmentMetadata segment0 = createSegmentUpdateWithState(remoteLogSegmentLifecycleManager, Collections.singletonMap(0, 0L), 0,
                                                                             100,
                                                                             RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
            RemoteLogSegmentMetadata segment1 = createSegmentUpdateWithState(remoteLogSegmentLifecycleManager, Collections.singletonMap(0, 101L), 101,
                                                                             200,
                                                                             RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
            Map<Integer, Long> segment2LeaderEpochs = new HashMap<>();
            segment2LeaderEpochs.put(0, 201L);
            segment2LeaderEpochs.put(1, 301L);
            RemoteLogSegmentMetadata segment2 = createSegmentUpdateWithState(remoteLogSegmentLifecycleManager, segment2LeaderEpochs, 201, 400,
                                                                             RemoteLogSegmentState.COPY_SEGMENT_FINISHED);

            // listRemoteLogSegments(0) and listAllRemoteLogSegments() should contain all the above segments.
            List<RemoteLogSegmentMetadata> expectedSegmentsForEpoch0 = Arrays.asList(segment0, segment1, segment2);
            assertTrue(TestUtils.sameElementsWithOrder(remoteLogSegmentLifecycleManager.listRemoteLogSegments(0),
                                                                  expectedSegmentsForEpoch0.iterator()));
            assertTrue(TestUtils.sameElementsWithoutOrder(remoteLogSegmentLifecycleManager.listAllRemoteLogSegments(),
                                                                     expectedSegmentsForEpoch0.iterator()));

            // listRemoteLogSegments(1) should contain only segment2.
            List<RemoteLogSegmentMetadata> expectedSegmentsForEpoch1 = Collections.singletonList(segment2);
            assertTrue(TestUtils.sameElementsWithOrder(remoteLogSegmentLifecycleManager.listRemoteLogSegments(1),
                                                                  expectedSegmentsForEpoch1.iterator()));
        } finally {
            Utils.closeQuietly(remoteLogSegmentLifecycleManager, "RemoteLogSegmentLifecycleManager");
        }
    }

    @ParameterizedTest(name = "testSegmentsLifeCycleInCache = {0}")
    @MethodSource("remoteLogSegmentLifecycleManagers")
    public void testSegmentsLifeCycleInCache(RemoteLogSegmentLifecycleManager remoteLogSegmentLifecycleManager) throws Exception {
        try {
            remoteLogSegmentLifecycleManager.initialize(topicIdPartition);

            // segment 0
            // offsets: [0-100]
            // leader epochs (0,0), (1,20), (2,80)
            Map<Integer, Long> segment0LeaderEpochs = new HashMap<>();
            segment0LeaderEpochs.put(0, 0L);
            segment0LeaderEpochs.put(1, 20L);
            segment0LeaderEpochs.put(2, 80L);
            RemoteLogSegmentId segment0Id = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
            RemoteLogSegmentMetadata segment0Metadata = new RemoteLogSegmentMetadata(segment0Id, 0L, 100L,
                                                                                     -1L, RemoteLogSegmentLifecycleTest.BROKER_ID_0, time.milliseconds(), RemoteLogSegmentLifecycleTest.SEG_SIZE,
                                                                                     segment0LeaderEpochs);
            remoteLogSegmentLifecycleManager.addRemoteLogSegmentMetadata(segment0Metadata);

            // We should not get this as the segment is still getting copied and it is not yet considered successful until
            // it reaches RemoteLogSegmentState.COPY_SEGMENT_FINISHED.
            assertFalse(remoteLogSegmentLifecycleManager.remoteLogSegmentMetadata(40, 1).isPresent());

            // Check that these leader epochs are not to be considered for highestOffsetForEpoch API as they are still getting copied.
            Stream.of(0, 1, 2).forEach(epoch -> {
                try {
                    assertFalse(remoteLogSegmentLifecycleManager.highestOffsetForEpoch(epoch).isPresent());
                } catch (RemoteStorageException e) {
                    fail(e);
                }
            });

            RemoteLogSegmentMetadataUpdate segment0Update = new RemoteLogSegmentMetadataUpdate(
                    segment0Id, time.milliseconds(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, RemoteLogSegmentLifecycleTest.BROKER_ID_1);
            remoteLogSegmentLifecycleManager.updateRemoteLogSegmentMetadata(segment0Update);
            RemoteLogSegmentMetadata expectedSegment0Metadata = segment0Metadata.createWithUpdates(segment0Update);

            // segment 1
            // offsets: [101 - 200]
            // no changes in leadership with in this segment
            // leader epochs (2, 101)
            Map<Integer, Long> segment1LeaderEpochs = Collections.singletonMap(2, 101L);
            RemoteLogSegmentMetadata segment1Metadata = createSegmentUpdateWithState(remoteLogSegmentLifecycleManager, segment1LeaderEpochs, 101L,
                                                                                     200L,
                                                                                     RemoteLogSegmentState.COPY_SEGMENT_FINISHED);

            // segment 2
            // offsets: [201 - 300]
            // moved to epoch 3 in between
            // leader epochs (2, 201), (3, 240)
            Map<Integer, Long> segment2LeaderEpochs = new HashMap<>();
            segment2LeaderEpochs.put(2, 201L);
            segment2LeaderEpochs.put(3, 240L);
            RemoteLogSegmentMetadata segment2Metadata = createSegmentUpdateWithState(remoteLogSegmentLifecycleManager, segment2LeaderEpochs, 201L,
                                                                                     300L,
                                                                                     RemoteLogSegmentState.COPY_SEGMENT_FINISHED);

            // segment 3
            // offsets: [250 - 400]
            // leader epochs (3, 250), (4, 370)
            Map<Integer, Long> segment3LeaderEpochs = new HashMap<>();
            segment3LeaderEpochs.put(3, 250L);
            segment3LeaderEpochs.put(4, 370L);
            RemoteLogSegmentMetadata segment3Metadata = createSegmentUpdateWithState(remoteLogSegmentLifecycleManager, segment3LeaderEpochs, 250L,
                                                                                     400L,
                                                                                     RemoteLogSegmentState.COPY_SEGMENT_FINISHED);

            //////////////////////////////////////////////////////////////////////////////////////////
            // Four segments are added with different boundaries and leader epochs.
            // Search for cache.remoteLogSegmentMetadata(leaderEpoch, offset)  for different
            // epochs and offsets
            //////////////////////////////////////////////////////////////////////////////////////////

            HashMap<RemoteLogSegmentLifecycleTest.EpochOffset, RemoteLogSegmentMetadata> expectedEpochOffsetToSegmentMetadata = new HashMap<>();
            // Existing metadata entries.
            expectedEpochOffsetToSegmentMetadata.put(new RemoteLogSegmentLifecycleTest.EpochOffset(1, 40), expectedSegment0Metadata);
            expectedEpochOffsetToSegmentMetadata.put(new RemoteLogSegmentLifecycleTest.EpochOffset(2, 110), segment1Metadata);
            expectedEpochOffsetToSegmentMetadata.put(new RemoteLogSegmentLifecycleTest.EpochOffset(3, 240), segment2Metadata);
            expectedEpochOffsetToSegmentMetadata.put(new RemoteLogSegmentLifecycleTest.EpochOffset(3, 250), segment3Metadata);
            expectedEpochOffsetToSegmentMetadata.put(new RemoteLogSegmentLifecycleTest.EpochOffset(4, 375), segment3Metadata);

            // Non existing metadata entries.
            // Search for offset 110, epoch 1, and it should not exist.
            expectedEpochOffsetToSegmentMetadata.put(new RemoteLogSegmentLifecycleTest.EpochOffset(1, 110), null);
            // Search for non existing offset 401, epoch 4.
            expectedEpochOffsetToSegmentMetadata.put(new RemoteLogSegmentLifecycleTest.EpochOffset(4, 401), null);
            // Search for non existing epoch 5.
            expectedEpochOffsetToSegmentMetadata.put(new RemoteLogSegmentLifecycleTest.EpochOffset(5, 301), null);

            for (Map.Entry<RemoteLogSegmentLifecycleTest.EpochOffset, RemoteLogSegmentMetadata> entry : expectedEpochOffsetToSegmentMetadata.entrySet()) {
                RemoteLogSegmentLifecycleTest.EpochOffset epochOffset = entry.getKey();
                Optional<RemoteLogSegmentMetadata> segmentMetadata = remoteLogSegmentLifecycleManager
                        .remoteLogSegmentMetadata(epochOffset.epoch, epochOffset.offset);
                RemoteLogSegmentMetadata expectedSegmentMetadata = entry.getValue();
                RemoteLogSegmentLifecycleTest.log.debug("Searching for {} , result: {}, expected: {} ", epochOffset, segmentMetadata,
                          expectedSegmentMetadata);
                if (expectedSegmentMetadata != null) {
                    assertEquals(Optional.of(expectedSegmentMetadata), segmentMetadata);
                } else {
                    assertFalse(segmentMetadata.isPresent());
                }
            }

            // Update segment with state as DELETE_SEGMENT_STARTED.
            // It should not be available when we search for that segment.
            remoteLogSegmentLifecycleManager
                    .updateRemoteLogSegmentMetadata(new RemoteLogSegmentMetadataUpdate(expectedSegment0Metadata.remoteLogSegmentId(),
                                                                                       time.milliseconds(),
                                                                                       RemoteLogSegmentState.DELETE_SEGMENT_STARTED,
                                                                                       RemoteLogSegmentLifecycleTest.BROKER_ID_1));
            assertFalse(remoteLogSegmentLifecycleManager.remoteLogSegmentMetadata(0, 10).isPresent());

            // Update segment with state as DELETE_SEGMENT_FINISHED.
            // It should not be available when we search for that segment.
            remoteLogSegmentLifecycleManager
                    .updateRemoteLogSegmentMetadata(new RemoteLogSegmentMetadataUpdate(expectedSegment0Metadata.remoteLogSegmentId(),
                                                                                       time.milliseconds(),
                                                                                       RemoteLogSegmentState.DELETE_SEGMENT_FINISHED,
                                                                                       RemoteLogSegmentLifecycleTest.BROKER_ID_1));
            assertFalse(remoteLogSegmentLifecycleManager.remoteLogSegmentMetadata(0, 10).isPresent());

            //////////////////////////////////////////////////////////////////////////////////////////
            //  Search for cache.highestLogOffset(leaderEpoch) for all the leader epochs
            //////////////////////////////////////////////////////////////////////////////////////////

            Map<Integer, Long> expectedEpochToHighestOffset = new HashMap<>();
            expectedEpochToHighestOffset.put(0, 19L);
            expectedEpochToHighestOffset.put(1, 79L);
            expectedEpochToHighestOffset.put(2, 239L);
            expectedEpochToHighestOffset.put(3, 369L);
            expectedEpochToHighestOffset.put(4, 400L);

            for (Map.Entry<Integer, Long> entry : expectedEpochToHighestOffset.entrySet()) {
                Integer epoch = entry.getKey();
                Long expectedOffset = entry.getValue();
                Optional<Long> offset = remoteLogSegmentLifecycleManager.highestOffsetForEpoch(epoch);
                RemoteLogSegmentLifecycleTest.log.debug("Fetching highest offset for epoch: {} , returned: {} , expected: {}", epoch, offset, expectedOffset);
                assertEquals(Optional.of(expectedOffset), offset);
            }

            // Search for non existing leader epoch
            Optional<Long> highestOffsetForEpoch5 = remoteLogSegmentLifecycleManager.highestOffsetForEpoch(5);
            assertFalse(highestOffsetForEpoch5.isPresent());
        } finally {
            Utils.closeQuietly(remoteLogSegmentLifecycleManager, "RemoteLogSegmentLifecycleManager");
        }
    }

}