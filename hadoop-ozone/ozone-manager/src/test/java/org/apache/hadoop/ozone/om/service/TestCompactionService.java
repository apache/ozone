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

package org.apache.hadoop.ozone.om.service;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_COMPACTION_SERVICE_RUN_INTERVAL;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.DBConfigFromFile;
import org.apache.hadoop.hdds.utils.db.TypedTable;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(OrderAnnotation.class)
class TestCompactionService {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestCompactionService.class);

  private static final int SERVICE_INTERVAL = 1;
  private static final int WAIT_TIME = (int) Duration.ofSeconds(10).toMillis();

  @BeforeAll
  void setup(@TempDir Path tempDir) throws Exception {
    ExitUtils.disableSystemExit();

    OzoneConfiguration conf = new OzoneConfiguration();
    System.setProperty(DBConfigFromFile.CONFIG_DIR, "/");
    ServerUtils.setOzoneMetaDirPath(conf, tempDir.toString());
    conf.setTimeDuration(OZONE_OM_COMPACTION_SERVICE_RUN_INTERVAL,
        SERVICE_INTERVAL, TimeUnit.MILLISECONDS);
    conf.setQuietMode(false);
  }

  /**
   * Add a compaction request and verify that it is processed.
   *
   * @throws IOException - on Failure.
   */
  @Timeout(300)
  @Test
  public void testCompact() throws Exception {
    OzoneManager ozoneManager = mock(OzoneManager.class);
    OMMetadataManager metadataManager = mock(OMMetadataManager.class);

    TypedTable table = mock(TypedTable.class);

    Set<String> tables = new HashSet<>();
    tables.add("keyTable");
    tables.add("fileTable");
    tables.add("directoryTable");
    tables.add("deletedTable");
    tables.add("deletedDirectoryTable");
    tables.add("multipartInfoTable");

    when(ozoneManager.getMetadataManager()).thenReturn(metadataManager);
    when(metadataManager.getTable(anyString())).thenReturn(table);
    when(metadataManager.listTableNames()).thenReturn(tables);

    CompactionService compactionService = new CompactionService(ozoneManager, TimeUnit.MILLISECONDS,
        TimeUnit.SECONDS.toMillis(SERVICE_INTERVAL), TimeUnit.SECONDS.toMillis(60),
        Arrays.asList(OMConfigKeys.OZONE_OM_COMPACTION_SERVICE_COLUMNFAMILIES_DEFAULT.split(","))) {

      @Override
      public void compactFully(String tableName) throws IOException {
        LOG.info("Compacting column family: {}", tableName);
      }
    };
    compactionService.start();

    compactionService.suspend();
    // wait for submitted tasks to complete
    Thread.sleep(SERVICE_INTERVAL);
    final long oldkeyCount = compactionService.getNumCompactions();
    LOG.info("oldkeyCount={}", oldkeyCount);

    final int compactionTriggered = 1;

    compactionService.resume();

    GenericTestUtils.waitFor(
        () -> compactionService.getNumCompactions() >= oldkeyCount + compactionTriggered,
        SERVICE_INTERVAL, WAIT_TIME);
  }

}
