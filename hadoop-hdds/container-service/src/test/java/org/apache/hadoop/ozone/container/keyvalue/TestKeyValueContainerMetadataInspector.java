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

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerInspector;
import org.apache.hadoop.ozone.container.common.utils.ContainerInspectorUtil;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.log4j.PatternLayout;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;

/**
 * Tests for {@link KeyValueContainerMetadataInspector}.
 */
@RunWith(Parameterized.class)
public class TestKeyValueContainerMetadataInspector
    extends TestKeyValueContainerIntegrityChecks {
  private static final long CONTAINER_ID = 102;

  public TestKeyValueContainerMetadataInspector(ContainerLayoutTestInfo
      containerLayoutTestInfo) {
    super(containerLayoutTestInfo);
  }

  @Test
  public void testRunDisabled() throws Exception {
    // Create incorrect container.
    KeyValueContainer container = createClosedContainer(3);
    KeyValueContainerData containerData = container.getContainerData();
    setDBBlockAndByteCounts(containerData, -2, -2);

    // No system property set. Should not run.
    System.clearProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY);
    ContainerInspectorUtil.load();
    Assert.assertNull(runInspectorAndGetReport(containerData));
    ContainerInspectorUtil.unload();

    // Unloaded. Should not run even with system property.
    System.setProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY,
        KeyValueContainerMetadataInspector.Mode.INSPECT.toString());
    Assert.assertNull(runInspectorAndGetReport(containerData));

    // Unloaded and no system property. Should not run.
    System.clearProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY);
    Assert.assertNull(runInspectorAndGetReport(containerData));
  }

  @Test
  public void testSystemPropertyAndReadOnly() {
    System.clearProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY);
    ContainerInspector inspector = new KeyValueContainerMetadataInspector();
    Assert.assertFalse(inspector.load());
    Assert.assertTrue(inspector.isReadOnly());

    // Inspect mode: valid argument and readonly.
    System.setProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY,
        KeyValueContainerMetadataInspector.Mode.INSPECT.toString());
    inspector = new KeyValueContainerMetadataInspector();
    Assert.assertTrue(inspector.load());
    Assert.assertTrue(inspector.isReadOnly());

    // Repair mode: valid argument and not readonly.
    System.setProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY,
        KeyValueContainerMetadataInspector.Mode.REPAIR.toString());
    inspector = new KeyValueContainerMetadataInspector();
    Assert.assertTrue(inspector.load());
    Assert.assertFalse(inspector.isReadOnly());

    // Bad argument: invalid argument and readonly.
    System.setProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY,
        "badvalue");
    inspector = new KeyValueContainerMetadataInspector();
    Assert.assertFalse(inspector.load());
    Assert.assertTrue(inspector.isReadOnly());

    // Clean slate for other tests.
    System.clearProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY);
  }

  @Test
  public void testMissingChunksDir() throws Exception {
    // Create container with missing chunks dir.
    // The metadata in the DB will not be set in this fake container.
    KeyValueContainer container = createClosedContainer(0);
    KeyValueContainerData containerData = container.getContainerData();
    String chunksDirStr = containerData.getChunksPath();
    File chunksDirFile = new File(chunksDirStr);
    FileUtils.deleteDirectory(chunksDirFile);
    Assert.assertFalse(chunksDirFile.exists());

    // In inspect mode, missing chunks dir should be detected but not fixed.
    JsonObject inspectJson = runInspectorAndGetReport(containerData,
        KeyValueContainerMetadataInspector.Mode.INSPECT);
    // The block count and used bytes should be null in this container, but
    // because it has no block keys that should not be an error.
    Assert.assertEquals(1,
        inspectJson.getAsJsonArray("errors").size());
    checkJsonErrorsReport(inspectJson, "chunksDirectory.present",
        new JsonPrimitive(true), new JsonPrimitive(false), false);
    Assert.assertFalse(chunksDirFile.exists());

    // In repair mode, missing chunks dir should be detected and fixed.
    JsonObject repairJson = runInspectorAndGetReport(containerData,
        KeyValueContainerMetadataInspector.Mode.REPAIR);
    Assert.assertEquals(1,
        inspectJson.getAsJsonArray("errors").size());
    checkJsonErrorsReport(repairJson, "chunksDirectory.present",
        new JsonPrimitive(true), new JsonPrimitive(false), true);
    Assert.assertTrue(chunksDirFile.exists());
    Assert.assertTrue(chunksDirFile.isDirectory());
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

    // Make sure it runs on open containers too.
    KeyValueContainer container = createOpenContainer(createBlocks);
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

    KeyValueContainer container = createClosedContainer(createBlocks);
    setDBBlockAndByteCounts(container.getContainerData(), createBlocks,
        setBytes);
    inspectThenRepairOnCorrectContainer(container.getContainerData());
  }

  public void inspectThenRepairOnCorrectContainer(
      KeyValueContainerData containerData) throws Exception {
    // No output for correct containers.
    Assert.assertNull(runInspectorAndGetReport(containerData,
        KeyValueContainerMetadataInspector.Mode.INSPECT));

    Assert.assertNull(runInspectorAndGetReport(containerData,
        KeyValueContainerMetadataInspector.Mode.REPAIR));
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

    String containerState = containerData.getState().toString();

    // First inspect the container.
    JsonObject inspectJson = runInspectorAndGetReport(containerData,
        KeyValueContainerMetadataInspector.Mode.INSPECT);

    checkJsonReportForIncorrectContainer(inspectJson,
        containerState, createdBlocks, setBlocks, createdBytes, setBytes,
        createdFiles, false);
    // Container should not have been modified in inspect mode.
    checkDBBlockAndByteCounts(containerData, setBlocks, setBytes);

    // Now repair the container.
    JsonObject repairJson = runInspectorAndGetReport(containerData,
        KeyValueContainerMetadataInspector.Mode.REPAIR);
    checkJsonReportForIncorrectContainer(repairJson,
        containerState, createdBlocks, setBlocks, createdBytes, setBytes,
        createdFiles, true);
    // Metadata keys should have been fixed.
    checkDBBlockAndByteCounts(containerData, createdBlocks, createdBytes);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private void checkJsonReportForIncorrectContainer(JsonObject inspectJson,
      String expectedContainerState, long createdBlocks,
      long setBlocks, long createdBytes, long setBytes, long createdFiles,
      boolean shouldRepair) {
    // Check main container properties.
    Assert.assertEquals(inspectJson.get("containerID").getAsLong(),
        CONTAINER_ID);
    Assert.assertEquals(inspectJson.get("containerState").getAsString(),
        expectedContainerState);

    // Check DB metadata.
    JsonObject jsonDbMetadata = inspectJson.getAsJsonObject("dBMetadata");
    Assert.assertEquals(setBlocks,
        jsonDbMetadata.get(OzoneConsts.BLOCK_COUNT).getAsLong());
    Assert.assertEquals(setBytes,
        jsonDbMetadata.get(OzoneConsts.CONTAINER_BYTES_USED).getAsLong());

    // Check aggregate metadata values.
    JsonObject jsonAggregates = inspectJson.getAsJsonObject("aggregates");
    Assert.assertEquals(createdBlocks,
        jsonAggregates.get("blockCount").getAsLong());
    Assert.assertEquals(createdBytes,
        jsonAggregates.get("usedBytes").getAsLong());
    Assert.assertEquals(0,
        jsonAggregates.get("pendingDeleteBlocks").getAsLong());

    // Check chunks directory.
    JsonObject jsonChunksDir = inspectJson.getAsJsonObject("chunksDirectory");
    Assert.assertTrue(jsonChunksDir.get("present").getAsBoolean());
    Assert.assertEquals(createdFiles,
        jsonChunksDir.get("fileCount").getAsLong());

    // Check errors.
    checkJsonErrorsReport(inspectJson, "dBMetadata.#BLOCKCOUNT",
        new JsonPrimitive(createdBlocks), new JsonPrimitive(setBlocks),
        shouldRepair);
    checkJsonErrorsReport(inspectJson, "dBMetadata.#BYTESUSED",
        new JsonPrimitive(createdBytes), new JsonPrimitive(setBytes),
        shouldRepair);
  }

  /**
   * Checks the erorr list in the provided JsonReport for an error matching
   * the template passed in with the parameters.
   */
  private void checkJsonErrorsReport(JsonObject jsonReport,
      String propertyValue, JsonPrimitive correctExpected,
      JsonPrimitive correctActual, boolean correctRepair) {

    Assert.assertFalse(jsonReport.get("correct").getAsBoolean());

    JsonArray jsonErrors = jsonReport.getAsJsonArray("errors");
    boolean matchFound = false;
    for (JsonElement jsonErrorElem: jsonErrors) {
      JsonObject jsonErrorObject = jsonErrorElem.getAsJsonObject();
      String thisProperty =
          jsonErrorObject.get("property").getAsString();

      if (thisProperty.equals(propertyValue)) {
        matchFound = true;

        JsonPrimitive expectedJsonPrim =
            jsonErrorObject.get("expected").getAsJsonPrimitive();
        Assert.assertEquals(correctExpected, expectedJsonPrim);

        JsonPrimitive actualJsonPrim =
            jsonErrorObject.get("actual").getAsJsonPrimitive();
        Assert.assertEquals(correctActual, actualJsonPrim);

        boolean repaired =
            jsonErrorObject.get("repaired").getAsBoolean();
        Assert.assertEquals(correctRepair, repaired);
        break;
      }
    }

    Assert.assertTrue(matchFound);
  }

  public void setDBBlockAndByteCounts(KeyValueContainerData containerData,
      long blockCount, long byteCount) throws Exception {
    try (ReferenceCountedDB db = BlockUtils.getDB(containerData, getConf())) {
      Table<String, Long> metadataTable = db.getStore().getMetadataTable();
      // Don't care about in memory state. Just change the DB values.
      metadataTable.put(OzoneConsts.BLOCK_COUNT, blockCount);
      metadataTable.put(OzoneConsts.CONTAINER_BYTES_USED, byteCount);
    }
  }

  public void checkDBBlockAndByteCounts(KeyValueContainerData containerData,
      long expectedBlockCount, long expectedBytesUsed) throws Exception {
    try (ReferenceCountedDB db = BlockUtils.getDB(containerData, getConf())) {
      Table<String, Long> metadataTable = db.getStore().getMetadataTable();

      long bytesUsed = metadataTable.get(OzoneConsts.CONTAINER_BYTES_USED);
      Assert.assertEquals(expectedBytesUsed, bytesUsed);

      long blockCount = metadataTable.get(OzoneConsts.BLOCK_COUNT);
      Assert.assertEquals(expectedBlockCount, blockCount);
    }
  }

  private JsonObject runInspectorAndGetReport(
      KeyValueContainerData containerData,
      KeyValueContainerMetadataInspector.Mode mode) throws Exception {
    System.setProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY,
        mode.toString());
    ContainerInspectorUtil.load();
    JsonObject json = runInspectorAndGetReport(containerData);
    ContainerInspectorUtil.unload();
    System.clearProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY);

    return json;
  }

  private JsonObject runInspectorAndGetReport(
      KeyValueContainerData containerData) throws Exception {
    // Use an empty layout so the captured log has no prefix and can be
    // parsed as json.
    GenericTestUtils.LogCapturer capturer =
        GenericTestUtils.LogCapturer.captureLogs(
            KeyValueContainerMetadataInspector.REPORT_LOG, new PatternLayout());
    KeyValueContainerUtil.parseKVContainerData(containerData, getConf());
    capturer.stopCapturing();
    String output = capturer.getOutput();
    capturer.clearOutput();

    return new Gson().fromJson(output, JsonObject.class);
  }

  private KeyValueContainer createClosedContainer(int normalBlocks)
      throws Exception {
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
