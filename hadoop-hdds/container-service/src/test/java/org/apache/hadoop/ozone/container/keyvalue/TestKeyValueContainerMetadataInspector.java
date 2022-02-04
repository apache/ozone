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
package org.apache.hadoop.ozone.container.keyvalue;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class TestKeyValueContainerMetadataInspector
    extends TestKeyValueContainerIntegrityChecks {
  private static final long containerID = 102;

  public TestKeyValueContainerMetadataInspector(ChunkLayoutTestInfo
      chunkManagerTestInfo) {
    super(chunkManagerTestInfo);
  }

  @Test
  public void testRunWithoutSystemProperty() throws Exception {
    System.clearProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY);
    Assert.assertFalse(new KeyValueContainerMetadataInspector().load());
    createClosedContainer(3);
    String log = runInspectorAndGetLog();
    Assert.assertFalse(log.contains(KeyValueContainerMetadataInspector.class.
        getSimpleName()));
  }

  @Test
  public void testSystemProperty() {
    Assert.assertEquals(ContainerProtos.ContainerType.KeyValueContainer,
        new KeyValueContainerMetadataInspector().getContainerType());
    System.clearProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY);
    Assert.assertFalse(new KeyValueContainerMetadataInspector().load());

    System.setProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY,
        KeyValueContainerMetadataInspector.Mode.INSPECT.toString());
    Assert.assertTrue(new KeyValueContainerMetadataInspector().load());
    Assert.assertTrue(new KeyValueContainerMetadataInspector().isReadOnly());

    new KeyValueContainerMetadataInspector().unload();
    Assert.assertNull(
        System.getProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY));

    System.setProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY,
        KeyValueContainerMetadataInspector.Mode.REPAIR.toString());
    Assert.assertTrue(new KeyValueContainerMetadataInspector().load());
    Assert.assertFalse(new KeyValueContainerMetadataInspector().isReadOnly());

    System.setProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY,
        "badvalue");
    Assert.assertFalse(new KeyValueContainerMetadataInspector().load());
    // Clean slate for other tests.
    System.clearProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY);
  }

  @Test
  public void testMissingChunksDir() throws Exception {
    createClosedContainer(0);
    String chunksDirStr = containerData.getChunksPath();
    File chunksDirFile = new File(chunksDirStr);
    FileUtils.deleteDirectory(chunksDirFile);
    Assert.assertFalse(chunksDirFile.exists());

    String inspectOutput = inspectContainerAndGetLog();
    Assert.assertTrue(inspectOutput.contains("!Missing chunks directory: " +
        chunksDirStr));
    // No repair should have been done.
    Assert.assertFalse(chunksDirFile.exists());

    String repairOutput = repairContainerAndGetLog();
    Assert.assertTrue(repairOutput.contains("!Missing chunks directory: " +
        chunksDirStr));
    Assert.assertTrue(repairOutput.contains("!Creating empty " +
        "chunks directory: " + chunksDirStr));
    Assert.assertTrue(chunksDirFile.exists());
  }

  @Test
  public void testIncorrectTotalsNoData() throws Exception {
    // Test incorrect totals on empty container.
    inspectThenRepair(0, -3, -3);
  }

  @Test
  public void testIncorrectTotalsWithData() throws Exception {
    inspectThenRepair(3, 4, -2);
  }

  @Test
  public void testCorrectTotalsNoData() throws Exception {
    inspectThenRepair(0, 0, 0);
  }

  @Test
  public void testCorrectTotalsWithData() throws Exception {
    inspectThenRepair(3, 3, 10);
  }

  public void inspectThenRepair(int createBlocks, int setBlocks,
      int setBytes) throws Exception {
    int createBytes = chunkLen * chunksPerBlock * createBlocks;
    int createFiles = 0;
    switch (chunkManagerTestInfo.getLayout()) {
      case FILE_PER_BLOCK:
        createFiles = createBlocks;
        break;
      case FILE_PER_CHUNK:
        createFiles = createBlocks * chunksPerBlock;
        break;
      default:
        Assert.fail("Unrecognized chunk layout version.");
    }

    createClosedContainer(createBlocks);
    setDBBlockAndByteCounts(setBlocks, setBytes);

    // First inspect the container.
    String inspectOutput = inspectContainerAndGetLog();
    String[] expectedInspectMessages = new String[]{
        "Audit of container " + containerID + " metadata",
        "#BLOCKCOUNT: " + setBlocks,
        "#BYTESUSED: " + setBytes,
        createFiles + " files in chunks directory",
        "Total block keys in DB: " + createBlocks,
        "Total pending delete block keys in DB: 0",
        "Schema Version: " + OzoneConsts.SCHEMA_V2,
        "Total used bytes in DB: " + createBytes,
    };
    containsAllStrings(inspectOutput, expectedInspectMessages);
    // Container should not have been modified in inspect mode.
    checkDBBlockAndByteCounts(setBlocks, setBytes);

    // Now repair the container. Output should be a superset of inspect output.
    String repairOutput = repairContainerAndGetLog();
    if (setBlocks != createBlocks) {
      String inspect = String.format(
          "!Value of metadata key #BLOCKCOUNT does not match DB " +
          "total: %d != %d", setBlocks, createBlocks);
      String repair = String.format("!Repairing #BLOCKCOUNT of %d to match " +
          "database " +
          "total: " +
          "%d", setBlocks, createBlocks);
      Assert.assertTrue(inspectOutput.contains(inspect));
      Assert.assertTrue(repairOutput.contains(repair));
    }
    if (setBytes != createBytes) {
      String inspect = String.format(
          "!Value of metadata key #BYTESUSED does not match DB total: %d" +
          " != %d", setBytes, createBytes);
      String repair = String.format("!Repairing #BYTESUSED of %s to match " +
          "database total: %s", setBytes, createBytes);
      Assert.assertTrue(inspectOutput.contains(inspect));
      Assert.assertTrue(repairOutput.contains(repair));
    }

    containsAllStrings(repairOutput, expectedInspectMessages);
    checkDBBlockAndByteCounts(createBlocks, createBytes);
  }

  public void setDBBlockAndByteCounts(long blockCount, long byteCount)
      throws Exception {
    try (ReferenceCountedDB db = BlockUtils.getDB(containerData, conf)) {
      Table<String, Long> metadataTable =
          db.getStore().getMetadataTable();
      // Don't care about in memory state. Just make the DB aggregates
      // incorrect.
      metadataTable.put(OzoneConsts.BLOCK_COUNT, blockCount);
      metadataTable.put(OzoneConsts.CONTAINER_BYTES_USED, byteCount);
    }
  }

  public void checkDBBlockAndByteCounts(long expectedBlockCount,
      long expectedBytesUsed) throws Exception {
    try (ReferenceCountedDB db = BlockUtils.getDB(containerData, conf)) {
      Table<String, Long> metadataTable =
          db.getStore().getMetadataTable();
      long bytesUsed = metadataTable.get(OzoneConsts.CONTAINER_BYTES_USED);
      Assert.assertEquals(expectedBytesUsed, bytesUsed);
      long blockCount = metadataTable.get(OzoneConsts.BLOCK_COUNT);
      Assert.assertEquals(expectedBlockCount, blockCount);
    }
  }

  private String inspectContainerAndGetLog() throws Exception {
    System.setProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY,
        KeyValueContainerMetadataInspector.Mode.INSPECT.toString());
    String logOut = runInspectorAndGetLog();
    System.clearProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY);
    return logOut;
  }

  private String repairContainerAndGetLog() throws Exception {
    System.setProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY,
        KeyValueContainerMetadataInspector.Mode.REPAIR.toString());
    String logOut = runInspectorAndGetLog();
    System.clearProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY);
    return logOut;
  }

  private String runInspectorAndGetLog() throws Exception {
    GenericTestUtils.LogCapturer capturer =
        GenericTestUtils.LogCapturer.captureLogs(
            KeyValueContainerMetadataInspector.LOG);
    new KeyValueContainerMetadataInspector().load();
    KeyValueContainerUtil.parseKVContainerData(containerData, conf);
    new KeyValueContainerMetadataInspector().unload();
    capturer.stopCapturing();
    String output = capturer.getOutput();
    capturer.clearOutput();
    return output;
  }

  private void createClosedContainer(int normalBlocks)
      throws Exception{
    createOpenContainer(normalBlocks);
    container.close();
  }

  private void createOpenContainer(int normalBlocks)
      throws Exception {
    super.createContainerWithBlocks(containerID, normalBlocks, 0);
  }

  private void containsAllStrings(String logOutput, String[] expectedMessages) {
    for (String expectedMessage : expectedMessages) {
      Assert.assertTrue("Log output did not contain \"" +
              expectedMessage + "\"", logOutput.contains(expectedMessage));
    }
  }
}
