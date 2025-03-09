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

package org.apache.hadoop.ozone.om.snapshot.filter;

import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;

/**
 * Test class for ReclaimableFilter.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestReclaimableFilter {

  private ReclaimableFilter<?> reclaimableFilter;
  private OzoneManager ozoneManager;
  private OmSnapshotManager omSnapshotManager;
  private SnapshotChainManager chainManager;
  private KeyManager keyManager;
  private IOzoneManagerLock ozoneManagerLock;



  protected ReclaimableFilter<?> initializeFilter(OzoneManager om, OmSnapshotManager snapshotManager,
                                                  SnapshotChainManager snapshotChainManager,
                                                  SnapshotInfo currentSnapshotInfo, KeyManager km,
                                                  IOzoneManagerLock lock, int numberOfPreviousSnapshotsFromChain) {
    return new ReclaimableFilter<Boolean>(om, snapshotManager, snapshotChainManager, currentSnapshotInfo,
        km, lock, numberOfPreviousSnapshotsFromChain) {
      @Override
      protected String getVolumeName(Table.KeyValue<String, Boolean> keyValue) throws IOException {
        return keyValue.getKey().split("/")[0];
      }

      @Override
      protected String getBucketName(Table.KeyValue<String, Boolean> keyValue) throws IOException {
        return keyValue.getKey().split("/")[1];
      }

      @Override
      protected Boolean isReclaimable(Table.KeyValue<String, Boolean> keyValue) throws IOException {
        return keyValue.getValue();
      }
    };
  }

  public void setup(SnapshotInfo currentSnapshotInfo, int numberOfPreviousSnapshotsFromChain) {
    this.ozoneManager = Mockito.mock(OzoneManager.class);
    this.omSnapshotManager = Mockito.mock(OmSnapshotManager.class);
    this.chainManager = Mockito.mock(SnapshotChainManager.class);
    this.keyManager = Mockito.mock(KeyManager.class);
    this.ozoneManagerLock = Mockito.mock(IOzoneManagerLock.class);
    this.reclaimableFilter = initializeFilter(ozoneManager, omSnapshotManager, chainManager,
        currentSnapshotInfo, keyManager, ozoneManagerLock, numberOfPreviousSnapshotsFromChain);
  }

  @AfterAll
  public void teardown() {

  }

  @Test
  public void testReclaimableFilter() {

  }

}
