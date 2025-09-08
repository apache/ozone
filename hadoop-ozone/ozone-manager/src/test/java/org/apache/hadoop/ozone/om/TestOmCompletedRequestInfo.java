/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo.OperationArgs;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;

/**
 * Tests OmCompletedRequestInfo om database table for Ozone object storage operations.
 */
public class TestOmCompletedRequestInfo {

  private OMMetadataManager omMetadataManager;
  private static final long EXPECTED_OPERATION_ID = 123L;
  private static final String EXPECTED_OPERATION_KEY = "123";

  private static final String VOLUME_NAME = "vol1";
  private static final String BUCKET_NAME = "bucket1";
  private static final String KEY_NAME = "bucket1";
  private static final long CLIENT_ID = 321L;

  @TempDir
  private Path folder;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_OM_DB_DIRS,
        folder.toAbsolutePath().toString());
    omMetadataManager = new OmMetadataManagerImpl(conf, null);
  }

  private OmCompletedRequestInfo createRequestInfo() {
    return new OmCompletedRequestInfo.Builder()
        .setTrxLogIndex(123L)
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setKeyName(KEY_NAME)
        .setCreationTime(Time.now())
        .setOpArgs(new OperationArgs.CreateKeyArgs())
        .build();
  }

  @Test
  public void testTableExists() throws Exception {
    Table<String, OmCompletedRequestInfo> requestInfo =
        omMetadataManager.getCompletedRequestInfoTable();
    Assertions.assertTrue(requestInfo.isEmpty());
  }

  @Test
  public void testAddNewOperation() throws Exception {
    Table<String, OmCompletedRequestInfo> requestInfo =
        omMetadataManager.getCompletedRequestInfoTable();
    requestInfo.put(EXPECTED_OPERATION_KEY, createRequestInfo());
    Assertions.assertEquals(EXPECTED_OPERATION_ID,
        requestInfo.get(EXPECTED_OPERATION_KEY).getTrxLogIndex());
  }

  @Test
  public void testDeleteOmCompletedRequestInfo() throws Exception {
    Table<String, OmCompletedRequestInfo> requestInfo =
        omMetadataManager.getCompletedRequestInfoTable();

    Assertions.assertFalse(requestInfo.isExist(EXPECTED_OPERATION_KEY));
    requestInfo.put(EXPECTED_OPERATION_KEY, createRequestInfo());
    Assertions.assertTrue(requestInfo.isExist(EXPECTED_OPERATION_KEY));
    requestInfo.delete(EXPECTED_OPERATION_KEY);
    Assertions.assertFalse(requestInfo.isExist(EXPECTED_OPERATION_KEY));
  }
}
