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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo.OperationArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests OmCompletedRequestInfo om database table for Ozone object storage operations.
 */
public class TestOmCompletedRequestInfo {

  private OMMetadataManager omMetadataManager;
  private static final long EXPECTED_OPERATION_ID = 123L;

  private static final String VOLUME_NAME = "vol1";
  private static final String BUCKET_NAME = "bucket1";
  private static final String KEY_NAME = "bucket1";

  @TempDir
  private Path folder;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_OM_DB_DIRS,
        folder.toAbsolutePath().toString());
    omMetadataManager = new OmMetadataManagerImpl(conf, null);
  }

  private OmCompletedRequestInfo createRequestInfo(long trxLogIndex) {
    return new OmCompletedRequestInfo.Builder()
        .setTrxLogIndex(trxLogIndex)
        .setCmdType(Type.CreateKey)
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setKeyName(KEY_NAME)
        .setCreationTime(Time.now())
        .setOpArgs(new OperationArgs.CreateKeyArgs())
        .build();
  }

  @Test
  public void testTableExists() throws Exception {
    Table<Long, OmCompletedRequestInfo> requestInfoTable =
        omMetadataManager.getCompletedRequestInfoTable();
    Assertions.assertTrue(requestInfoTable.isEmpty());
  }

  @Test
  public void testAddNewOperation() throws Exception {
    Table<Long, OmCompletedRequestInfo> requestInfoTable =
        omMetadataManager.getCompletedRequestInfoTable();
    requestInfoTable.put(EXPECTED_OPERATION_ID, createRequestInfo(EXPECTED_OPERATION_ID));
    Assertions.assertEquals(EXPECTED_OPERATION_ID,
        requestInfoTable.get(EXPECTED_OPERATION_ID).getTrxLogIndex());
  }

  @Test
  public void testDeleteOmCompletedRequestInfo() throws Exception {
    Table<Long, OmCompletedRequestInfo> requestInfoTable =
        omMetadataManager.getCompletedRequestInfoTable();

    Assertions.assertFalse(requestInfoTable.isExist(EXPECTED_OPERATION_ID));
    requestInfoTable.put(EXPECTED_OPERATION_ID, createRequestInfo(EXPECTED_OPERATION_ID));
    Assertions.assertTrue(requestInfoTable.isExist(EXPECTED_OPERATION_ID));
    requestInfoTable.delete(EXPECTED_OPERATION_ID);
    Assertions.assertFalse(requestInfoTable.isExist(EXPECTED_OPERATION_ID));
  }

  @Test
  // This is a little outside of the scope of a unit test for this
  // entity class but it seems fundamental to this entity storage that
  // the iteration order is expected
  public void testIterationOrderIsBasedOnTransationIndex() throws Exception {
    Table<Long, OmCompletedRequestInfo> requestInfoTable =
        omMetadataManager.getCompletedRequestInfoTable();

    List<Long> idsToInsert = Arrays.asList(
        1L, 19L, 8L, 7L, 111L, 72L, 992L, 11L, 42L);
    List<Long> expectedIdIterationOrder = Arrays.asList(
        1L, 7L, 8L, 11L, 19L, 42L, 72L, 111L, 992L);

    for (Long trxLogIndex : idsToInsert) {
      OmCompletedRequestInfo requestInfo = createRequestInfo(trxLogIndex);
      requestInfoTable.put(trxLogIndex, requestInfo);
    }

    List<Long> iteratedIds = new ArrayList<>();
    Table.KeyValue<Long, OmCompletedRequestInfo> requestInfoRow;

    try (TableIterator<Long, ? extends Table.KeyValue<Long, OmCompletedRequestInfo>>
            tableIterator = requestInfoTable.iterator()) {

      while (tableIterator.hasNext()) {
        requestInfoRow = tableIterator.next();
        iteratedIds.add(requestInfoRow.getValue().getTrxLogIndex());
      }
    }

    Assertions.assertEquals(iteratedIds, expectedIdIterationOrder);
  }
}
