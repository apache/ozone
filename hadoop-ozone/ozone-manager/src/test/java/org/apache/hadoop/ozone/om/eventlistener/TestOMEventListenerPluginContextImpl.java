/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.eventlistener;

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests {@link OMEventListenerPluginContextImpl}.
 */
@ExtendWith(MockitoExtension.class)
public class TestOMEventListenerPluginContextImpl {

  @Mock
  private OzoneManager ozoneManager;

  @Mock
  private OMMetadataManager metadataManager;

  @Mock
  private NotificationCheckpointStrategy checkpointStrategy;

  @Mock
  private Table<Long, OmCompletedRequestInfo> completedRequestInfoTable;

  private OMEventListenerPluginContextImpl pluginContext;

  @BeforeEach
  public void setup() {
    when(ozoneManager.getMetadataManager()).thenReturn(metadataManager);
    when(metadataManager.getCompletedRequestInfoTable()).thenReturn(completedRequestInfoTable);

    pluginContext = new OMEventListenerPluginContextImpl(ozoneManager, checkpointStrategy);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPruningCoordinatesMultipleCheckpoints() throws Exception {
    // 1. Mock checkpointStrategy to return minimum checkpoint = 80
    when(checkpointStrategy.getMinimumCheckpoint()).thenReturn(80L);

    // 2. Mock completedRequestInfoTable iterator to return latest transaction log index = 200
    Table.KeyValueIterator<Long, OmCompletedRequestInfo> completedRequestIterator =
        mock(Table.KeyValueIterator.class);
    when(completedRequestInfoTable.iterator()).thenReturn(completedRequestIterator);
    Table.KeyValue<Long, OmCompletedRequestInfo> latestEntry = mock(Table.KeyValue.class);
    when(latestEntry.getKey()).thenReturn(200L);
    when(completedRequestIterator.hasNext()).thenReturn(true, false);
    when(completedRequestIterator.next()).thenReturn((Table.KeyValue) latestEntry);

    // 3. Execute coordinated pruning
    // softLimit = 5, hardLimit = 50
    // minCheckpoint is 80. softPruneBoundary = 80 - 5 = 75
    // latestKey is 200. hardPruneBoundary = 200 - 50 = 150
    // beforeKey = max(75, 150) = 150
    pluginContext.pruneCompletedRequestInfo(5, 50);

    // Verify deleteRange is called up to 150L
    verify(completedRequestInfoTable).deleteRange(eq(0L), eq(150L));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPruningRespectsOldestCheckpointWhenSlowerThanHardLimit() throws Exception {
    // 1. Mock checkpointStrategy to return minimum checkpoint = 160 (e.g. oldest across kafka=180, audit=160)
    when(checkpointStrategy.getMinimumCheckpoint()).thenReturn(160L);

    // 2. Mock completedRequestInfoTable iterator to return latest transaction log index = 200
    Table.KeyValueIterator<Long, OmCompletedRequestInfo> completedRequestIterator =
        mock(Table.KeyValueIterator.class);
    when(completedRequestInfoTable.iterator()).thenReturn(completedRequestIterator);
    Table.KeyValue<Long, OmCompletedRequestInfo> latestEntry = mock(Table.KeyValue.class);
    when(latestEntry.getKey()).thenReturn(200L);
    when(completedRequestIterator.hasNext()).thenReturn(true, false);
    when(completedRequestIterator.next()).thenReturn((Table.KeyValue) latestEntry);

    // 3. Execute coordinated pruning
    // softLimit = 10, hardLimit = 80
    // minCheckpoint is 160. softPruneBoundary = 160 - 10 = 150
    // latestKey is 200. hardPruneBoundary = 200 - 80 = 120
    // beforeKey = max(150, 120) = 150
    pluginContext.pruneCompletedRequestInfo(10, 80);

    // Verify deleteRange is called up to 150L (respecting the soft-prune boundary from the oldest checkpoint)
    verify(completedRequestInfoTable).deleteRange(eq(0L), eq(150L));
  }
}
