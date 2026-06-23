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

package org.apache.hadoop.ozone.debug.datanode.container.analyze;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link ScmContainerMetadataReader}.
 */
public class TestScmContainerMetadataReader {

  @TempDir
  private Path tempDir;

  private OzoneConfiguration conf;
  private ContainerAnalyzeTestHelper testHelper;

  @BeforeEach
  public void setup() {
    conf = new OzoneConfiguration();
    testHelper = new ContainerAnalyzeTestHelper(tempDir, conf,
        UUID.randomUUID().toString(), UUID.randomUUID().toString());
  }

  @Test
  public void testClassifyNotInScm() throws Exception {
    File scmDb = testHelper.createScmDb(Collections.emptyMap());
    try (ScmContainerMetadataReader reader = new ScmContainerMetadataReader(conf, scmDb)) {
      Optional<ScmContainerMetadataReader.ScmContainerClassification> result = reader.classify(1001L);
      assertTrue(result.isPresent());
      assertEquals(ScmContainerMetadataReader.ScmContainerClassification.NOT_IN_SCM, result.get());
    }
  }

  @Test
  public void testClassifyDeleted() throws Exception {
    Map<Long, HddsProtos.LifeCycleState> containers = new HashMap<>();
    containers.put(1002L, HddsProtos.LifeCycleState.DELETED);
    File scmDb = testHelper.createScmDb(containers);

    try (ScmContainerMetadataReader reader = new ScmContainerMetadataReader(conf, scmDb.getParentFile())) {
      Optional<ScmContainerMetadataReader.ScmContainerClassification> result = reader.classify(1002L);
      assertTrue(result.isPresent());
      assertEquals(ScmContainerMetadataReader.ScmContainerClassification.DELETED, result.get());
    }
  }

  @Test
  public void testClassifyOmitOther() throws Exception {
    Map<Long, HddsProtos.LifeCycleState> containers = new HashMap<>();
    containers.put(1003L, HddsProtos.LifeCycleState.CLOSED);
    containers.put(1004L, HddsProtos.LifeCycleState.OPEN);
    File scmDb = testHelper.createScmDb(containers);

    try (ScmContainerMetadataReader reader = new ScmContainerMetadataReader(conf, scmDb)) {
      assertFalse(reader.classify(1003L).isPresent());
      assertFalse(reader.classify(1004L).isPresent());
    }
  }

  @Test
  public void testResolveScmDbDirectoryReturnsAbsolutePathWithParent() throws Exception {
    File scmDb = testHelper.createScmDb(Collections.emptyMap());
    File resolved = ScmContainerMetadataReader.resolveScmDbDirectory(scmDb);
    assertTrue(resolved.isAbsolute());
    assertNotNull(resolved.getParentFile());
    assertEquals(scmDb.getAbsolutePath(), resolved.getAbsolutePath());
  }
}
