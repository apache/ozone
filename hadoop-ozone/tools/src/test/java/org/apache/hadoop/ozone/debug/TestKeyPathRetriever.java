/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.debug;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the ozone key path retriever command.
 */
public class TestKeyPathRetriever {

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;
  private static final String DEFAULT_ENCODING = UTF_8.name();

  private static final String OZONE_USER = "ozone";
  private static final String OLD_USER = System.getProperty("user.name");

  @BeforeEach
  public void setup() throws Exception {
    System.setOut(new PrintStream(out, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(err, false, DEFAULT_ENCODING));
    System.setProperty("user.name", OZONE_USER);
  }

  @AfterEach
  public void reset() {
    // reset stream after each unit test
    out.reset();
    err.reset();

    // restore system streams
    System.setOut(OLD_OUT);
    System.setErr(OLD_ERR);
    System.setProperty("user.name", OLD_USER);
  }

  @Test
  void testRetriveFullPath() throws Exception {
    KeyPathRetriever keyPathRetriever = new KeyPathRetriever();

    Set<Long> containerSet = new HashSet<>();
    containerSet.add(500L);
    containerSet.add(800L);
    PrintWriter writer = new PrintWriter(out);

    Map<Long, String> dirInfoMap = new HashMap<>();
    Table<Long, String> dirTreeTable = mock(Table.class);
    keyPathRetriever.setDirTreeTable(dirTreeTable);

    doAnswer(invocation -> {
      Long objectId = invocation.getArgument(0);
      String val = invocation.getArgument(1);
      dirInfoMap.put(objectId, val);
      return null;
    }).when(dirTreeTable).put(anyLong(), anyString());

    when(dirTreeTable.get(anyLong())).thenAnswer(inv -> dirInfoMap.get((Long)inv.getArgument(0)));

    OMMetadataManager mock = mock(OMMetadataManager.class);
    when(mock.getVolumeId(anyString())).thenReturn(1L);

    // bucket iteration mock
    Table<String, OmBucketInfo> bucketTblMock = mock(Table.class);
    TableIterator bucketTblItr = mock(TableIterator.class);
    when(mock.getBucketTable()).thenReturn(bucketTblMock);
    when(bucketTblMock.iterator()).thenReturn(bucketTblItr);
    when(bucketTblItr.hasNext()).thenReturn(true, false);
    when(bucketTblItr.next()).thenReturn(Table.newKeyValue("bucket1",
        new OmBucketInfo.Builder().setBucketName("bucket1").setVolumeName("vol").setObjectID(2).build()));

    // dir iteration mock
    Table<String, OmDirectoryInfo> dirTblMock = mock(Table.class);
    TableIterator dirTblItr = mock(TableIterator.class);
    when(mock.getDirectoryTable()).thenReturn(dirTblMock);
    when(dirTblMock.iterator()).thenReturn(dirTblItr);
    when(dirTblItr.hasNext()).thenReturn(true, false);
    when(dirTblItr.next()).thenReturn(Table.newKeyValue("/1/2/2/dir1",
        new OmDirectoryInfo.Builder().setName("dir1").setParentObjectID(2).setObjectID(3).build()));

    // file iteration mock
    Table<String, OmKeyInfo> fileTblMock = mock(Table.class);
    TableIterator fileTblItr = mock(TableIterator.class);
    when(mock.getFileTable()).thenReturn(fileTblMock);
    when(fileTblMock.iterator()).thenReturn(fileTblItr);
    when(fileTblItr.hasNext()).thenReturn(true, true, true, true, false);
    Table.KeyValue<String, OmKeyInfo> file1 = Table.newKeyValue("1/2/3/file1",
        new OmKeyInfo.Builder().setKeyName("file1").setParentObjectID(3).setObjectID(4)
        .setOmKeyLocationInfos(getLocationGrpList(500)).build());
    Table.KeyValue<String, OmKeyInfo> file2 = Table.newKeyValue("1/2/3/file2",
        new OmKeyInfo.Builder().setKeyName("file2").setParentObjectID(3).setObjectID(5)
            .setOmKeyLocationInfos(getLocationGrpList(500)).build());
    Table.KeyValue<String, OmKeyInfo> file3 = Table.newKeyValue("1/2/3/file3",
        new OmKeyInfo.Builder().setKeyName("file3").setParentObjectID(3).setObjectID(6)
            .setOmKeyLocationInfos(getLocationGrpList(600)).build());
    Table.KeyValue<String, OmKeyInfo> file4 = Table.newKeyValue("1/2/3/file4",
        new OmKeyInfo.Builder().setKeyName("file4").setParentObjectID(3).setObjectID(7)
            .setOmKeyLocationInfos(getLocationGrpList(800)).build());
    when(fileTblItr.next()).thenReturn(file1, file2, file3, file4);

    keyPathRetriever.retrieve(mock, writer, containerSet);
    writer.close();
    assertThat(out.toString()).contains("vol/bucket1/dir1/file1").contains("vol/bucket1/dir1/file2")
        .contains("vol/bucket1/dir1/file4").doesNotContain("file3");
  }

  private static List<OmKeyLocationInfoGroup> getLocationGrpList(long containerId) {
    List<OmKeyLocationInfoGroup> locationGrpList = new ArrayList<>();
    List<OmKeyLocationInfo> locationList = new ArrayList<>();
    locationList.add(new OmKeyLocationInfo.Builder().setBlockID(new BlockID(containerId, 500L)).build());
    locationGrpList.add(new OmKeyLocationInfoGroup(1, locationList));
    return locationGrpList;
  }
}
