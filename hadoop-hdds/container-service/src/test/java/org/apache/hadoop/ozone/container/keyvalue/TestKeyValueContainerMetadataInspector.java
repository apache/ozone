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
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * Tests for {@link KeyValueContainerMetadataInspector}.
 */
public class TestKeyValueContainerMetadataInspector
    extends TestKeyValueContainerIntegrityChecks {
  private static final long CONTAINER_ID = 102;

  public TestKeyValueContainerMetadataInspector(ContainerLayoutTestInfo
      containerLayoutTestInfo) {
    super(containerLayoutTestInfo);
  }

  @Test
  public void testRunWithoutSystemProperty() throws Exception {
    System.clearProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY);
    Assert.assertFalse(new KeyValueContainerMetadataInspector().load());
    KeyValueContainer container = createClosedContainer(3);
    KeyValueContainerData containerData = container.getContainerData();
    String log = runInspectorAndGetLog(containerData);
    Assert.assertFalse(log.contains(KeyValueContainerMetadataInspector.class.
        getSimpleName()));
  }

  @Test
  public void testSystemProperty() {
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
    KeyValueContainer container = createClosedContainer(0);
    KeyValueContainerData containerData = container.getContainerData();
    String chunksDirStr = containerData.getChunksPath();
    File chunksDirFile = new File(chunksDirStr);
    FileUtils.deleteDirectory(chunksDirFile);
    Assert.assertFalse(chunksDirFile.exists());

    String inspectOutput = inspectContainerAndGetLog(containerData);
    Assert.assertTrue(inspectOutput.contains("!Missing chunks directory: " +
        chunksDirStr));
    // No repair should have been done.
    Assert.assertFalse(chunksDirFile.exists());

    String repairOutput = repairContainerAndGetLog(containerData);
    Assert.assertTrue(repairOutput.contains("!Missing chunks directory: " +
        chunksDirStr));
    Assert.assertTrue(repairOutput.contains("!Creating empty " +
        "chunks directory: " + chunksDirStr));
    Assert.assertTrue(chunksDirFile.exists());
  }

  @Test
  public void testIncorrectTotalsNoData() throws Exception {
    int createBlocks = 0;
    int setBlocks = -3;
    int setBytes = -2;

    KeyValueContainer container = createClosedContainer(createBlocks);
    setDBBlockAndByteCounts(container.getContainerData(), setBlocks, setBytes);
    inspectThenRepairOnIncorrectContainer(container.getContainerData(),
        createBlocks, setBlocks, setBytes);
  }

  @Test
  public void testIncorrectTotalsWithData() throws Exception {
    int createBlocks = 3;
    int setBlocks = 4;
    int setBytes = -2;

    KeyValueContainer container = createClosedContainer(createBlocks);
    setDBBlockAndByteCounts(container.getContainerData(), setBlocks, setBytes);
    inspectThenRepairOnIncorrectContainer(container.getContainerData(),
        createBlocks, setBlocks, setBytes);
  }

  @Test
  public void testCorrectTotalsNoData() throws Exception {
    int createBlocks = 0;
    int setBytes = 0;

    KeyValueContainer container = createClosedContainer(createBlocks);
    setDBBlockAndByteCounts(container.getContainerData(), createBlocks,
        setBytes);
    inspectThenRepairOnCorrectContainer(container.getContainerData());
  }

  @Test
  public void testCorrectTotalsWithData() throws Exception {
    int createBlocks = 3;
    int setBytes = CHUNK_LEN * CHUNKS_PER_BLOCK * createBlocks;

    // Make sure it runs on open containers too.
    KeyValueContainer container = createOpenContainer(createBlocks);
    setDBBlockAndByteCounts(container.getContainerData(), createBlocks,
        setBytes);
    inspectThenRepairOnCorrectContainer(container.getContainerData());
  }

  public void inspectThenRepairOnCorrectContainer(
      KeyValueContainerData containerData) throws Exception {
    String inspectOutput = inspectContainerAndGetLog(containerData);
    Assert.assertFalse(inspectOutput.contains(
        KeyValueContainerMetadataInspector.class.getSimpleName()));

    String repairOutput = repairContainerAndGetLog(containerData);
    Assert.assertFalse(repairOutput.contains(
        KeyValueContainerMetadataInspector.class.getSimpleName()));
  }

  /**
   * Creates a container as specified by the parameters.
   * Runs the inspector in inspect mode and checks the output.
   * Runs the inspector in repair mode and checks the output.
   *
   * @param createdBlocks Number of blocks to create in the container.
   * @param setBlocks total block count value set in the database.
   * @param setBytes total used bytes value set in the database.
   */
  public void inspectThenRepairOnIncorrectContainer(
      KeyValueContainerData containerData, int createdBlocks, int setBlocks,
      int setBytes) throws Exception {
    int createdBytes = CHUNK_LEN * CHUNKS_PER_BLOCK * createdBlocks;
    int createdFiles = 0;
    switch (getChunkLayout()) {
    case FILE_PER_BLOCK:
      createdFiles = createdBlocks;
      break;
    case FILE_PER_CHUNK:
      createdFiles = createdBlocks * CHUNKS_PER_BLOCK;
      break;
    default:
      Assert.fail("Unrecognized chunk layout version.");
    }

    // First inspect the container.
    String inspectOutput = inspectContainerAndGetLog(containerData);
    String[] expectedInspectMessages = new String[]{
        "Audit of container " + CONTAINER_ID + " metadata",
        "#BLOCKCOUNT: " + setBlocks,
        "#BYTESUSED: " + setBytes,
        createdFiles + " files in chunks directory",
        String.format(
        "!Value of metadata key #BLOCKCOUNT does not match DB " +
            "total: %d != %d", setBlocks, createdBlocks),
        String.format(
        "!Value of metadata key #BYTESUSED does not match DB total: %d" +
            " != %d", setBytes, createdBytes),
        "Total block keys in DB: " + createdBlocks,
        "Total pending delete block keys in DB: 0",
        "Schema Version: " + OzoneConsts.SCHEMA_V2,
        "Total used bytes in DB: " + createdBytes,
    };
    containsAllStrings(inspectOutput, expectedInspectMessages);
    // Container should not have been modified in inspect mode.
    checkDBBlockAndByteCounts(containerData, setBlocks, setBytes);

    // Now repair the container.
    String repairOutput = repairContainerAndGetLog(containerData);
    String[] expectedRepairMessages = new String[]{
      String.format("!Repairing #BLOCKCOUNT of %d to match " +
          "database total: %d", setBlocks, createdBlocks),
      String.format("!Repairing #BYTESUSED of %s to match " +
        "database total: %s", setBytes, createdBytes)
    };
    // Repair output should be a superset of inspect output.
    containsAllStrings(repairOutput, expectedInspectMessages);
    containsAllStrings(repairOutput, expectedRepairMessages);

    checkDBBlockAndByteCounts(containerData, createdBlocks, createdBytes);
  }

  public void setDBBlockAndByteCounts(KeyValueContainerData containerData,
      long blockCount, long byteCount) throws Exception {
    try (ReferenceCountedDB db = BlockUtils.getDB(containerData, getConf())) {
      Table<String, Long> metadataTable =
          db.getStore().getMetadataTable();
      // Don't care about in memory state. Just make the DB aggregates
      // incorrect.
      metadataTable.put(OzoneConsts.BLOCK_COUNT, blockCount);
      metadataTable.put(OzoneConsts.CONTAINER_BYTES_USED, byteCount);
    }
  }

  public void checkDBBlockAndByteCounts(KeyValueContainerData containerData,
      long expectedBlockCount, long expectedBytesUsed) throws Exception {
    try (ReferenceCountedDB db = BlockUtils.getDB(containerData, getConf())) {
      Table<String, Long> metadataTable =
          db.getStore().getMetadataTable();
      long bytesUsed = metadataTable.get(OzoneConsts.CONTAINER_BYTES_USED);
      Assert.assertEquals(expectedBytesUsed, bytesUsed);
      long blockCount = metadataTable.get(OzoneConsts.BLOCK_COUNT);
      Assert.assertEquals(expectedBlockCount, blockCount);
    }
  }

  private String inspectContainerAndGetLog(KeyValueContainerData containerData)
      throws Exception {
    System.setProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY,
        KeyValueContainerMetadataInspector.Mode.INSPECT.toString());
    String logOut = runInspectorAndGetLog(containerData);
    System.clearProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY);
    return logOut;
  }

  private String repairContainerAndGetLog(KeyValueContainerData containerData)
      throws Exception {
    System.setProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY,
        KeyValueContainerMetadataInspector.Mode.REPAIR.toString());
    String logOut = runInspectorAndGetLog(containerData);
    System.clearProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY);
    return logOut;
  }

  private String runInspectorAndGetLog(KeyValueContainerData containerData)
      throws Exception {
    GenericTestUtils.LogCapturer capturer =
        GenericTestUtils.LogCapturer.captureLogs(
            KeyValueContainerMetadataInspector.LOG);
    new KeyValueContainerMetadataInspector().load();
    KeyValueContainerUtil.parseKVContainerData(containerData, getConf());
    new KeyValueContainerMetadataInspector().unload();
    capturer.stopCapturing();
    String output = capturer.getOutput();
    capturer.clearOutput();
    return output;
  }

  private KeyValueContainer createClosedContainer(int normalBlocks)
      throws Exception{
    KeyValueContainer container = createOpenContainer(normalBlocks);
    container.close();
    return container;
  }

  private KeyValueContainer createOpenContainer(int normalBlocks)
      throws Exception {
    return super.createContainerWithBlocks(CONTAINER_ID, normalBlocks, 0);
  }

  private void containsAllStrings(String logOutput, String[] expectedMessages) {
    for (String expectedMessage : expectedMessages) {
      Assert.assertTrue("Log output did not contain \"" +
              expectedMessage + "\"", logOutput.contains(expectedMessage));
    }
  }
}
