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

package org.apache.hadoop.ozone.om.upgrade;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Path;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link ClearPreparedStateOmUpgradeAction}.
 */
class TestClearPreparedStateOmUpgradeAction {

  private static final String PREPARE_MARKER_KEY = "#PREPAREDINFO";

  @TempDir
  private Path tempDir;

  private OzoneManager mockOm(OzoneConfiguration conf) {
    OMMetadataManager metadataManager = mock(OMMetadataManager.class);
    @SuppressWarnings("unchecked")
    Table<String, TransactionInfo> txTable = mock(Table.class);
    when(metadataManager.getTransactionInfoTable()).thenReturn(txTable);

    OzoneManager om = mock(OzoneManager.class);
    when(om.getConfiguration()).thenReturn(conf);
    when(om.getMetadataManager()).thenReturn(metadataManager);
    return om;
  }

  @Test
  void testDeletesMarkerFileAndDbKey() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS, tempDir.toString());

    // Create the "current" dir and the marker file that pre-ZDU code would leave behind.
    File currentDir = new File(tempDir.toFile(), "current");
    assertTrue(currentDir.mkdirs());
    File marker = new File(currentDir, "prepareMarker");
    assertTrue(marker.createNewFile());

    OzoneManager om = mockOm(conf);
    new ClearPreparedStateOmUpgradeAction().execute(om);

    assertFalse(marker.exists(), "prepare marker file should be deleted");
    verify(om.getMetadataManager().getTransactionInfoTable()).delete(PREPARE_MARKER_KEY);
  }

  @Test
  void testIdempotentWhenNoMarkerPresent() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, tempDir.toString());

    OzoneManager om = mockOm(conf);
    // Should succeed without error even though the marker file and DB key are absent.
    new ClearPreparedStateOmUpgradeAction().execute(om);

    verify(om.getMetadataManager().getTransactionInfoTable()).delete(PREPARE_MARKER_KEY);
  }
}
