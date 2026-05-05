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

package org.apache.ozone.compaction.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hdds.StringUtils;
import org.apache.ozone.rocksdb.util.SstFileInfo;
import org.junit.jupiter.api.Test;
import org.rocksdb.LiveFileMetaData;

/**
 * Test class for Base SstFileInfo class.
 */
public class TestSstFileInfo {

  @Test
  public void testSstFileInfo() {
    String smallestKey = "/smallestKey/1";
    String largestKey = "/largestKey/2";
    String columnFamily = "columnFamily/123";
    LiveFileMetaData lfm = mock(LiveFileMetaData.class);
    when(lfm.fileName()).thenReturn("/1.sst");
    when(lfm.columnFamilyName()).thenReturn(StringUtils.string2Bytes(columnFamily));
    when(lfm.smallestKey()).thenReturn(StringUtils.string2Bytes(smallestKey));
    when(lfm.largestKey()).thenReturn(StringUtils.string2Bytes(largestKey));
    SstFileInfo expectedSstFileInfo = new SstFileInfo("1", smallestKey, largestKey, columnFamily);
    assertEquals(expectedSstFileInfo, new SstFileInfo(lfm));
  }
}
