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

package org.apache.hadoop.hdds.scm.pipeline;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.Test;

/**
 * Tests for storage-type-aware pipeline creation in
 * BackgroundPipelineCreator.
 */
public class TestBackgroundPipelineCreatorStorageType {

  @Test
  public void testStorageTypeAwareDisabled() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(
        ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_STORAGE_TYPE_AWARE,
        false);

    PipelineManager pipelineManager = mock(PipelineManager.class);
    when(pipelineManager.createPipeline(any(ReplicationConfig.class)))
        .thenThrow(new IOException("exhausted"));

    SCMContext scmContext = SCMContext.emptyContext();

    TestClock clock = new TestClock(Instant.now(), ZoneOffset.UTC);
    BackgroundPipelineCreator creator =
        new BackgroundPipelineCreator(pipelineManager, conf, scmContext,
            clock);

    creator.createPipelines();

    // Untyped createPipeline(ReplicationConfig) should have been called.
    verify(pipelineManager, atLeastOnce())
        .createPipeline(any(ReplicationConfig.class));
    // Typed createPipeline(ReplicationConfig, StorageType) should NOT
    // have been called.
    verify(pipelineManager, never())
        .createPipeline(any(ReplicationConfig.class),
            any(StorageType.class));
  }

  @Test
  public void testStorageTypeAwareEnabled() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(
        ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_STORAGE_TYPE_AWARE,
        true);

    PipelineManager pipelineManager = mock(PipelineManager.class);
    when(pipelineManager.createPipeline(any(ReplicationConfig.class)))
        .thenThrow(new IOException("exhausted"));
    when(pipelineManager.createPipeline(any(ReplicationConfig.class),
        any(StorageType.class)))
        .thenThrow(new IOException("exhausted"));

    SCMContext scmContext = SCMContext.emptyContext();

    TestClock clock = new TestClock(Instant.now(), ZoneOffset.UTC);
    BackgroundPipelineCreator creator =
        new BackgroundPipelineCreator(pipelineManager, conf, scmContext,
            clock);

    creator.createPipelines();

    // When storage-type-aware is enabled, the typed method should be called
    // for SSD, DISK, and ARCHIVE.
    verify(pipelineManager, atLeastOnce())
        .createPipeline(any(ReplicationConfig.class),
            any(StorageType.class));
  }
}
