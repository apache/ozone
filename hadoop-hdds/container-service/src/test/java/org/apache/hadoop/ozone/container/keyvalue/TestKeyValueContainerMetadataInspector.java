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

package org.apache.hadoop.ozone.container.keyvalue;

import static org.apache.ozone.test.GenericTestUtils.toLog4j;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.JsonTestUtils;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerInspector;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.utils.ContainerInspectorUtil;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaThreeImpl;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaTwoImpl;
import org.apache.log4j.PatternLayout;
import org.apache.ozone.test.GenericTestUtils;

/**
 * Tests for {@link KeyValueContainerMetadataInspector}.
 */
public class TestKeyValueContainerMetadataInspector extends TestKeyValueContainerIntegrityChecks {
  private static final long CONTAINER_ID = 102;

  static final DeletedBlocksTransactionGeneratorForTesting GENERATOR =
      new DeletedBlocksTransactionGeneratorForTesting();

  @ContainerTestVersionInfo.ContainerTest
  public void testRunDisabled(ContainerTestVersionInfo versionInfo)
      throws Exception {
    initTestData(versionInfo);
    // Create incorrect container.
    KeyValueContainer container = createClosedContainer(3);
    KeyValueContainerData containerData = container.getContainerData();
    setDBBlockAndByteCounts(containerData, -2, -2);

    // No system property set. Should not run.
    System.clearProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY);
    ContainerInspectorUtil.load();
    assertNull(runInspectorAndGetReport(containerData));
    ContainerInspectorUtil.unload();

    // Unloaded. Should not run even with system property.
    System.setProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY,
        KeyValueContainerMetadataInspector.Mode.INSPECT.toString());
    assertNull(runInspectorAndGetReport(containerData));

    // Unloaded and no system property. Should not run.
    System.clearProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY);
    assertNull(runInspectorAndGetReport(containerData));
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testSystemPropertyAndReadOnly(
      ContainerTestVersionInfo versionInfo) throws Exception {
    initTestData(versionInfo);
    System.clearProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY);
    ContainerInspector inspector = new KeyValueContainerMetadataInspector();
    assertFalse(inspector.load());
    assertTrue(inspector.isReadOnly());

    // Inspect mode: valid argument and readonly.
    System.setProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY,
        KeyValueContainerMetadataInspector.Mode.INSPECT.toString());
    inspector = new KeyValueContainerMetadataInspector();
    assertTrue(inspector.load());
    assertTrue(inspector.isReadOnly());

    // Repair mode: valid argument and not readonly.
    System.setProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY,
        KeyValueContainerMetadataInspector.Mode.REPAIR.toString());
    inspector = new KeyValueContainerMetadataInspector();
    assertTrue(inspector.load());
    assertFalse(inspector.isReadOnly());

    // Bad argument: invalid argument and readonly.
    System.setProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY,
        "badvalue");
    inspector = new KeyValueContainerMetadataInspector();
    assertFalse(inspector.load());
    assertTrue(inspector.isReadOnly());

    // Clean slate for other tests.
    System.clearProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY);
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testIncorrectTotalsNoData(ContainerTestVersionInfo versionInfo)
      throws Exception {
    initTestData(versionInfo);
    int createBlocks = 0;
    int setBlocks = -3;
    int setBytes = -2;

    KeyValueContainer container = createClosedContainer(createBlocks);
    setDBBlockAndByteCounts(container.getContainerData(), setBlocks, setBytes);
    inspectThenRepairOnIncorrectContainer(container.getContainerData(),
        createBlocks, setBlocks, setBytes, 0, 0);
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testIncorrectTotalsWithData(ContainerTestVersionInfo versionInfo)
      throws Exception {
    initTestData(versionInfo);
    int createBlocks = 3;
    int setBlocks = 4;
    int setBytes = -2;

    // Make sure it runs on open containers too.
    KeyValueContainer container = createOpenContainer(createBlocks);
    setDBBlockAndByteCounts(container.getContainerData(), setBlocks, setBytes);
    inspectThenRepairOnIncorrectContainer(container.getContainerData(),
        createBlocks, setBlocks, setBytes, 0, 0);
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testCorrectTotalsNoData(ContainerTestVersionInfo versionInfo)
      throws Exception {
    initTestData(versionInfo);
    int createBlocks = 0;
    int setBytes = 0;

    KeyValueContainer container = createClosedContainer(createBlocks);
    setDBBlockAndByteCounts(container.getContainerData(), createBlocks,
        setBytes);
    inspectThenRepairOnCorrectContainer(container.getContainerData());
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testCorrectTotalsWithData(ContainerTestVersionInfo versionInfo)
      throws Exception {
    initTestData(versionInfo);
    int createBlocks = 3;
    int setBytes = CHUNK_LEN * CHUNKS_PER_BLOCK * createBlocks;

    KeyValueContainer container = createClosedContainer(createBlocks);
    setDBBlockAndByteCounts(container.getContainerData(), createBlocks,
        setBytes);
    inspectThenRepairOnCorrectContainer(container.getContainerData());
  }

  static class DeletedBlocksTransactionGeneratorForTesting {
    private long txId = 100;
    private long localId = 2000;

    DeletedBlocksTransaction next(long containerId, int numBlocks) {
      final DeletedBlocksTransaction.Builder b
          = DeletedBlocksTransaction.newBuilder()
          .setContainerID(containerId)
          .setTxID(txId++)
          .setCount(0);
      for (int i = 0; i < numBlocks; i++) {
        b.addLocalID(localId++);
      }
      return b.build();
    }

    List<DeletedBlocksTransaction> generate(
        long containerId, List<Integer> numBlocks) {
      final List<DeletedBlocksTransaction> transactions = new ArrayList<>();
      for (int n : numBlocks) {
        transactions.add(next(containerId, n));
      }
      return transactions;
    }
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testCorrectDeleteWithTransaction(
      ContainerTestVersionInfo versionInfo) throws Exception {
    initTestData(versionInfo);
    final int createBlocks = 4;
    final int setBytes = CHUNK_LEN * CHUNKS_PER_BLOCK * createBlocks;
    final int deleteCount = 10;

    final KeyValueContainer container = createClosedContainer(createBlocks);
    final List<DeletedBlocksTransaction> deleteTransactions
        = GENERATOR.generate(container.getContainerData().getContainerID(),
        Arrays.asList(1, 6, 3));
    final long numDeletedLocalIds = deleteTransactions.stream()
        .mapToLong(DeletedBlocksTransaction::getLocalIDCount).sum();
    LOG.info("deleteTransactions = {}", deleteTransactions);
    LOG.info("numDeletedLocalIds = {}", numDeletedLocalIds);
    assertEquals(deleteCount, numDeletedLocalIds);

    setDB(container.getContainerData(), createBlocks,
        setBytes, deleteCount, deleteTransactions);
    inspectThenRepairOnCorrectContainer(container.getContainerData());
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testIncorrectDeleteWithTransaction(
      ContainerTestVersionInfo versionInfo) throws Exception {
    initTestData(versionInfo);
    final int createBlocks = 4;
    final int setBytes = CHUNK_LEN * CHUNKS_PER_BLOCK * createBlocks;
    final int deleteCount = 10;

    final KeyValueContainer container = createClosedContainer(createBlocks);
    final List<DeletedBlocksTransaction> deleteTransactions
        = GENERATOR.generate(container.getContainerData().getContainerID(),
        Arrays.asList(1, 3));
    final long numDeletedLocalIds = deleteTransactions.stream()
        .mapToLong(DeletedBlocksTransaction::getLocalIDCount).sum();
    LOG.info("deleteTransactions = {}", deleteTransactions);
    LOG.info("numDeletedLocalIds = {}", numDeletedLocalIds);

    setDB(container.getContainerData(), createBlocks,
        setBytes, deleteCount, deleteTransactions);
    inspectThenRepairOnIncorrectContainer(container.getContainerData(),
        createBlocks, createBlocks, setBytes,
        deleteCount, numDeletedLocalIds);
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testIncorrectDeleteWithoutTransaction(
      ContainerTestVersionInfo versionInfo) throws Exception {
    initTestData(versionInfo);
    final int createBlocks = 4;
    final int setBytes = CHUNK_LEN * CHUNKS_PER_BLOCK * createBlocks;
    final int deleteCount = 10;

    final KeyValueContainer container = createClosedContainer(createBlocks);
    final List<DeletedBlocksTransaction> deleteTransactions
        = Collections.emptyList();
    final long numDeletedLocalIds = deleteTransactions.stream()
        .mapToLong(DeletedBlocksTransaction::getLocalIDCount).sum();
    LOG.info("deleteTransactions = {}", deleteTransactions);
    LOG.info("numDeletedLocalIds = {}", numDeletedLocalIds);

    setDB(container.getContainerData(), createBlocks,
        setBytes, deleteCount, deleteTransactions);
    inspectThenRepairOnIncorrectContainer(container.getContainerData(),
        createBlocks, createBlocks, setBytes,
        deleteCount, numDeletedLocalIds);
  }

  public void inspectThenRepairOnCorrectContainer(
      KeyValueContainerData containerData) throws Exception {
    // No output for correct containers.
    assertNull(runInspectorAndGetReport(containerData,
        KeyValueContainerMetadataInspector.Mode.INSPECT));

    assertNull(runInspectorAndGetReport(containerData,
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
   * @param deleteCount total deleted block count value set in the database.
   * @param numDeletedLocalIds total number of deleted block local id count
   *                           in the transactions
   */
  public void inspectThenRepairOnIncorrectContainer(
      KeyValueContainerData containerData, int createdBlocks, int setBlocks,
      int setBytes, int deleteCount, long numDeletedLocalIds)
      throws Exception {
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
      fail("Unrecognized chunk layout version.");
    }

    String containerState = containerData.getState().toString();

    // First inspect the container.
    JsonNode inspectJson = runInspectorAndGetReport(containerData,
        KeyValueContainerMetadataInspector.Mode.INSPECT);

    checkJsonReportForIncorrectContainer(inspectJson,
        containerState, createdBlocks, setBlocks, createdBytes, setBytes,
        createdFiles, deleteCount, numDeletedLocalIds, false);
    // Container should not have been modified in inspect mode.
    checkDbCounts(containerData, setBlocks, setBytes, deleteCount);

    // Now repair the container.
    JsonNode repairJson = runInspectorAndGetReport(containerData,
        KeyValueContainerMetadataInspector.Mode.REPAIR);
    checkJsonReportForIncorrectContainer(repairJson,
        containerState, createdBlocks, setBlocks, createdBytes, setBytes,
        createdFiles, deleteCount, numDeletedLocalIds, true);
    // Metadata keys should have been fixed.
    checkDbCounts(containerData, createdBlocks, createdBytes,
        numDeletedLocalIds);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private void checkJsonReportForIncorrectContainer(JsonNode inspectJson,
      String expectedContainerState, long createdBlocks,
      long setBlocks, long createdBytes, long setBytes, long createdFiles,
      long setPendingDeleteCount, long createdPendingDeleteCount,
      boolean shouldRepair) {
    // Check main container properties.
    assertEquals(inspectJson.get("containerID").asLong(), CONTAINER_ID);
    assertEquals(inspectJson.get("containerState").asText(), expectedContainerState);

    // Check DB metadata.
    JsonNode jsonDbMetadata = inspectJson.get("dBMetadata");
    assertEquals(setBlocks,
        jsonDbMetadata.get(OzoneConsts.BLOCK_COUNT).asLong());
    assertEquals(setBytes,
        jsonDbMetadata.get(OzoneConsts.CONTAINER_BYTES_USED).asLong());

    // Check aggregate metadata values.
    JsonNode jsonAggregates = inspectJson.get("aggregates");
    assertEquals(createdBlocks,
        jsonAggregates.get("blockCount").asLong());
    assertEquals(createdBytes,
        jsonAggregates.get("usedBytes").asLong());
    assertEquals(createdPendingDeleteCount,
        jsonAggregates.get("pendingDeleteBlocks").asLong());

    // Check chunks directory.
    JsonNode jsonChunksDir = inspectJson.get("chunksDirectory");
    assertTrue(jsonChunksDir.get("present").asBoolean());
    assertEquals(createdFiles,
        jsonChunksDir.get("fileCount").asLong());

    // Check errors.
    checkJsonErrorsReport(inspectJson, "dBMetadata.#BLOCKCOUNT",
        createdBlocks, setBlocks, shouldRepair);
    checkJsonErrorsReport(inspectJson, "dBMetadata.#BYTESUSED",
        createdBytes, setBytes, shouldRepair);
    checkJsonErrorsReport(inspectJson, "dBMetadata.#PENDINGDELETEBLOCKCOUNT",
        createdPendingDeleteCount, setPendingDeleteCount, shouldRepair);
  }

  private void checkJsonErrorsReport(
      JsonNode jsonReport, String propertyValue,
      long correctExpected, long correctActual,
      boolean correctRepair) {
    if (correctExpected == correctActual) {
      return;
    }
    JsonNode correctExpectedNode = JsonTestUtils.valueToJsonNode(correctExpected);
    JsonNode correctActualNode = JsonTestUtils.valueToJsonNode(correctActual);

    checkJsonErrorsReport(jsonReport, propertyValue, correctExpectedNode,
        correctActualNode, correctRepair);
  }

  /**
   * Checks the erorr list in the provided JsonReport for an error matching
   * the template passed in with the parameters.
   */
  private void checkJsonErrorsReport(JsonNode jsonReport,
      String propertyValue, JsonNode correctExpected,
      JsonNode correctActual, boolean correctRepair) {

    assertFalse(jsonReport.get("correct").asBoolean());

    JsonNode jsonErrors = jsonReport.get("errors");
    boolean matchFound = false;
    for (JsonNode jsonErrorElem : jsonErrors) {
      String thisProperty =
          jsonErrorElem.get("property").asText();

      if (thisProperty.equals(propertyValue)) {
        matchFound = true;
        assertEquals(correctExpected.asLong(), jsonErrorElem.get("expected").asLong());
        assertEquals(correctActual.asLong(), jsonErrorElem.get("actual").asLong());

        boolean repaired = jsonErrorElem.get("repaired").asBoolean();
        assertEquals(correctRepair, repaired);
        break;
      }
    }

    assertTrue(matchFound);
  }

  public void setDBBlockAndByteCounts(KeyValueContainerData containerData,
      long blockCount, long byteCount) throws Exception {
    setDB(containerData, blockCount, byteCount,
        0, Collections.emptyList());
  }

  public void setDB(KeyValueContainerData containerData,
      long blockCount, long byteCount,
      long dbDeleteCount, List<DeletedBlocksTransaction> deleteTransactions)
      throws Exception {
    try (DBHandle db = BlockUtils.getDB(containerData, getConf())) {
      Table<String, Long> metadataTable = db.getStore().getMetadataTable();
      // Don't care about in memory state. Just change the DB values.
      metadataTable.put(containerData.getBlockCountKey(), blockCount);
      metadataTable.put(containerData.getBytesUsedKey(), byteCount);
      metadataTable.put(containerData.getPendingDeleteBlockCountKey(),
          dbDeleteCount);

      final DatanodeStore store = db.getStore();
      LOG.info("store {}", store.getClass().getSimpleName());
      if (store instanceof DatanodeStoreSchemaTwoImpl) {
        final DatanodeStoreSchemaTwoImpl s2store
            = (DatanodeStoreSchemaTwoImpl)store;
        final Table<Long, DeletedBlocksTransaction> delTxTable
            = s2store.getDeleteTransactionTable();
        try (BatchOperation batch = store.getBatchHandler()
            .initBatchOperation()) {
          for (DeletedBlocksTransaction t : deleteTransactions) {
            delTxTable.putWithBatch(batch, t.getTxID(), t);
          }
          store.getBatchHandler().commitBatchOperation(batch);
        }
      } else if (store instanceof DatanodeStoreSchemaThreeImpl) {
        final DatanodeStoreSchemaThreeImpl s3store
            = (DatanodeStoreSchemaThreeImpl)store;
        final Table<String, DeletedBlocksTransaction> delTxTable
            = s3store.getDeleteTransactionTable();
        try (BatchOperation batch = store.getBatchHandler()
            .initBatchOperation()) {
          for (DeletedBlocksTransaction t : deleteTransactions) {
            final String key = containerData.getDeleteTxnKey(t.getTxID());
            delTxTable.putWithBatch(batch, key, t);
          }
          store.getBatchHandler().commitBatchOperation(batch);
        }
      } else {
        throw new UnsupportedOperationException(
            "Unsupported store class " + store.getClass().getSimpleName());
      }
    }
  }

  void checkDbCounts(KeyValueContainerData containerData,
      long expectedBlockCount, long expectedBytesUsed,
      long expectedDeletedCount) throws Exception {
    try (DBHandle db = BlockUtils.getDB(containerData, getConf())) {
      Table<String, Long> metadataTable = db.getStore().getMetadataTable();

      long bytesUsed = metadataTable.get(containerData.getBytesUsedKey());
      assertEquals(expectedBytesUsed, bytesUsed);

      long blockCount = metadataTable.get(containerData.getBlockCountKey());
      assertEquals(expectedBlockCount, blockCount);

      final long deleteCount = metadataTable.get(
          containerData.getPendingDeleteBlockCountKey());
      assertEquals(expectedDeletedCount, deleteCount);
    }
  }

  private JsonNode runInspectorAndGetReport(
      KeyValueContainerData containerData,
      KeyValueContainerMetadataInspector.Mode mode) throws Exception {
    System.setProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY,
        mode.toString());
    ContainerInspectorUtil.load();

    JsonNode json = runInspectorAndGetReport(containerData);

    ContainerInspectorUtil.unload();
    System.clearProperty(KeyValueContainerMetadataInspector.SYSTEM_PROPERTY);

    return json;
  }

  private JsonNode runInspectorAndGetReport(
      KeyValueContainerData containerData) throws Exception {
    // Use an empty layout so the captured log has no prefix and can be
    // parsed as json.
    GenericTestUtils.LogCapturer capturer =
        GenericTestUtils.LogCapturer.captureLogs(
            toLog4j(KeyValueContainerMetadataInspector.REPORT_LOG),
            new PatternLayout());
    KeyValueContainerUtil.parseKVContainerData(containerData, getConf());
    capturer.stopCapturing();
    String output = capturer.getOutput();
    capturer.clearOutput();
    // Check if the output is effectively empty
    if (StringUtils.isBlank(output)) {
      return null;
    }
    return JsonTestUtils.readTree(output);
  }

  private KeyValueContainer createClosedContainer(int normalBlocks)
      throws Exception {
    KeyValueContainer container = createOpenContainer(normalBlocks);
    container.close();
    return container;
  }

  private KeyValueContainer createOpenContainer(int normalBlocks)
      throws Exception {
    return super.createContainerWithBlocks(CONTAINER_ID, normalBlocks, 0, true);
  }
}
