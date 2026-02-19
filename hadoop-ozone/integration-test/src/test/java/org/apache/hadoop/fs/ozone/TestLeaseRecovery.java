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

package org.apache.hadoop.fs.ozone;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_READ_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.FORCE_LEASE_RECOVERY_ENV;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_ROOT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.util.LinkedList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.utils.FaultInjectorImpl;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ozone.test.OzoneTestBase;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.event.Level;

/**
 * Test cases for recoverLease() API.
 */
@Flaky("HDDS-11323")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestLeaseRecovery extends OzoneTestBase {

  private MiniOzoneCluster cluster;

  private OzoneClient client;
  private final OzoneConfiguration conf = new OzoneConfiguration();
  private String dir;
  private Path file;
  private LogCapturer xceiverClientLogs;
  private RootedOzoneFileSystem fs;

  /**
   * Closing the output stream after lease recovery throws because the key
   * is no longer open in OM.  This is currently expected (see HDDS-9358).
   */
  public static void closeIgnoringKeyNotFound(OutputStream stream) {
    closeIgnoringOMException(stream, OMException.ResultCodes.KEY_NOT_FOUND);
  }

  public static void closeIgnoringOMException(OutputStream stream, OMException.ResultCodes expectedResultCode) {
    try {
      stream.close();
    } catch (OMException e) {
      assertEquals(expectedResultCode, e.getResult());
    } catch (Exception e) {
      OMException omException = assertInstanceOf(OMException.class, e.getCause());
      assertEquals(expectedResultCode, omException.getResult());
    }
  }

  @BeforeAll
  public void init() throws IOException, InterruptedException,
      TimeoutException {
    final int chunkSize = 16 << 10;
    final int flushSize = 2 * chunkSize;
    final int maxFlushSize = 2 * flushSize;
    final int blockSize = 2 * maxFlushSize;
    final BucketLayout layout = BucketLayout.FILE_SYSTEM_OPTIMIZED;

    conf.setBoolean(OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED, true);
    conf.setBoolean("ozone.client.hbase.enhancements.allowed", true);
    conf.setBoolean("fs." + OZONE_OFS_URI_SCHEME + ".impl.disable.cache", true);
    conf.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);
    conf.set(OZONE_DEFAULT_BUCKET_LAYOUT, layout.name());
    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 10);
    conf.set(OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT, "0s");
    // make sure flush will write data to DN
    conf.setBoolean("ozone.client.stream.buffer.flush.delay", false);

    ClientConfigForTesting.newBuilder(StorageUnit.BYTES)
        .setBlockSize(blockSize)
        .setChunkSize(chunkSize)
        .setStreamBufferFlushSize(flushSize)
        .setStreamBufferMaxSize(maxFlushSize)
        .setDataStreamBufferFlushSize(maxFlushSize)
        .setDataStreamMinPacketSize(chunkSize)
        .setDataStreamWindowSize(5 * chunkSize)
        .applyTo(conf);

    cluster = MiniOzoneCluster.newBuilder(conf)
      .setNumDatanodes(3)
      .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    // create a volume and a bucket to be used by OzoneFileSystem
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client, layout);

    GenericTestUtils.setLogLevel(XceiverClientGrpc.class, Level.DEBUG);

    // Set the fs.defaultFS
    final String rootPath = String.format("%s://%s/", OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    dir = OZONE_ROOT + bucket.getVolumeName() + OZONE_URI_DELIMITER + bucket.getName();

    xceiverClientLogs = LogCapturer.captureLogs(XceiverClientGrpc.class);
  }

  @BeforeEach
  void beforeEach() throws Exception {
    file = new Path(dir, uniqueObjectName());
    fs = (RootedOzoneFileSystem) FileSystem.get(conf);
  }

  @AfterEach
  void afterEach() {
    IOUtils.closeQuietly(fs);
    xceiverClientLogs.clearOutput();
    KeyValueHandler.setInjector(null);
  }

  @AfterAll
  public void tearDown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1 << 17, (1 << 17) + 1, (1 << 17) - 1})
  public void testRecovery(int dataSize) throws Exception {
    final byte[] data = getData(dataSize);

    final FSDataOutputStream stream = fs.create(file, true);
    try {
      stream.write(data);
      stream.hsync();
      assertFalse(fs.isFileClosed(file));

      // write more data without hsync
      stream.write(data);
      stream.flush();

      int count = 0;
      while (count++ < 15 && !fs.recoverLease(file)) {
        Thread.sleep(1000);
      }
      // The lease should have been recovered.
      assertTrue(fs.isFileClosed(file), "File should be closed");

      // A second call to recoverLease should succeed too.
      assertTrue(fs.recoverLease(file));
    } finally {
      closeIgnoringKeyNotFound(stream);
    }

    // open it again, make sure the data is correct
    verifyData(data, dataSize * 2, file, fs);
  }

  @Test
  public void testRecoveryWithoutHsyncHflushOnLastBlock() throws Exception {
    int blockSize = (int) cluster.getOzoneManager().getConfiguration().getStorageSize(
        OZONE_SCM_BLOCK_SIZE, OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);

    final byte[] data = getData(blockSize / 2 + 1);

    final FSDataOutputStream stream = fs.create(file, true);
    try {
      stream.write(data);
      stream.hsync();
      assertFalse(fs.isFileClosed(file));

      // It will write into new block as well
      // Don't do hsync/flush
      stream.write(data);

      int count = 0;
      while (count++ < 15 && !fs.recoverLease(file)) {
        Thread.sleep(1000);
      }
      // The lease should have been recovered.
      assertTrue(fs.isFileClosed(file), "File should be closed");

      // A second call to recoverLease should succeed too.
      assertTrue(fs.recoverLease(file));
    } finally {
      closeIgnoringKeyNotFound(stream);
    }

    // open it again, make sure the data is correct
    verifyData(data, blockSize / 2 + 1, file, fs);
  }

  @Test
  public void testOBSRecoveryShouldFail() throws Exception {
    OzoneBucket obsBucket = TestDataUtil.createVolumeAndBucket(client,
        "vol2", "obs", BucketLayout.OBJECT_STORE);
    String obsDir = OZONE_ROOT + obsBucket.getVolumeName() + OZONE_URI_DELIMITER + obsBucket.getName();
    Path obsFile = new Path(obsDir, uniqueObjectName());

    assertThrows(IllegalArgumentException.class, () -> fs.recoverLease(obsFile));
  }

  @Test
  public void testFinalizeBlockFailure() throws Exception {
    int dataSize = 100;
    final byte[] data = getData(dataSize);

    final FSDataOutputStream stream = fs.create(file, true);
    try {
      stream.write(data);
      stream.hsync();
      assertFalse(fs.isFileClosed(file));

      // write more data without hsync
      stream.write(data);
      stream.flush();

      FaultInjectorImpl injector = new FaultInjectorImpl();
      injector.setType(ContainerProtos.Type.FinalizeBlock);
      KeyValueHandler.setInjector(injector);
      StorageContainerException sce = new StorageContainerException(
          "Requested operation not allowed as ContainerState is CLOSED",
          ContainerProtos.Result.CLOSED_CONTAINER_IO);
      injector.setException(sce);
      LogCapturer logs =
          LogCapturer.captureLogs(BasicRootedOzoneClientAdapterImpl.class);

      fs.recoverLease(file);
      assertTrue(logs.getOutput().contains("Failed to execute finalizeBlock command"));
      assertTrue(logs.getOutput().contains("Requested operation not allowed as ContainerState is CLOSED"));

      // The lease should have been recovered.
      assertTrue(fs.isFileClosed(file), "File should be closed");
      FileStatus fileStatus = fs.getFileStatus(file);
      assertEquals(dataSize * 2, fileStatus.getLen());
    } finally {
      closeIgnoringKeyNotFound(stream);
    }

    // open it again, make sure the data is correct
    verifyData(data, dataSize * 2, file, fs);
  }

  @Test
  public void testBlockPipelineClosed() throws Exception {
    int dataSize = 100;
    final byte[] data = getData(dataSize);

    final FSDataOutputStream stream = fs.create(file, true);
    try {
      stream.write(data);
      stream.hsync();
      assertFalse(fs.isFileClosed(file));

      // write more data without hsync
      stream.write(data);
      stream.flush();

      // close the pipeline
      StorageContainerManager scm = cluster.getStorageContainerManager();
      ContainerInfo container = closeLatestContainer();
      GenericTestUtils.waitFor(() -> {
        try {
          return scm.getPipelineManager().getPipeline(container.getPipelineID()).isClosed();
        } catch (PipelineNotFoundException e) {
          throw new RuntimeException(e);
        }
      }, 200, 30000);

      fs.recoverLease(file);

      // The lease should have been recovered.
      assertTrue(fs.isFileClosed(file), "File should be closed");
      FileStatus fileStatus = fs.getFileStatus(file);
      assertEquals(dataSize * 2, fileStatus.getLen());
    } finally {
      closeIgnoringKeyNotFound(stream);
    }

    // open it again, make sure the data is correct
    verifyData(data, dataSize * 2, file, fs);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testGetCommittedBlockLengthTimeout(boolean forceRecovery) throws Exception {
    // reduce read timeout
    OzoneConfiguration clientConf = new OzoneConfiguration(conf);
    clientConf.set(OZONE_CLIENT_READ_TIMEOUT, "2s");
    // set force recovery
    System.setProperty(FORCE_LEASE_RECOVERY_ENV, String.valueOf(forceRecovery));
    try (RootedOzoneFileSystem fs = (RootedOzoneFileSystem)FileSystem.get(clientConf)) {
      int dataSize = 100;
      final byte[] data = getData(dataSize);

      final FSDataOutputStream stream = fs.create(file, true);
      try {
        stream.write(data);
        stream.hsync();
        assertFalse(fs.isFileClosed(file));

        // write more data without hsync
        stream.write(data);
        stream.flush();

        // close the pipeline and container
        closeLatestContainer();
        // pause getCommittedBlockLength handling on all DNs to make sure all getCommittedBlockLength will time out
        FaultInjectorImpl injector = new FaultInjectorImpl();
        injector.setType(ContainerProtos.Type.GetCommittedBlockLength);
        KeyValueHandler.setInjector(injector);
        if (!forceRecovery) {
          assertThrows(IOException.class, () -> fs.recoverLease(file));
          return;
        } else {
          fs.recoverLease(file);
        }
        assertEquals(3, StringUtils.countMatches(xceiverClientLogs.getOutput(),
            "Executing command cmdType: GetCommittedBlockLength"));

        // The lease should have been recovered.
        assertTrue(fs.isFileClosed(file), "File should be closed");
        FileStatus fileStatus = fs.getFileStatus(file);
        // Since all DNs are out, then the length in OM keyInfo will be used as the final file length
        assertEquals(dataSize, fileStatus.getLen());
      } finally {
        if (!forceRecovery) {
          closeIgnoringOMException(stream, OMException.ResultCodes.KEY_UNDER_LEASE_RECOVERY);
        } else {
          closeIgnoringKeyNotFound(stream);
        }
      }

      // open it again, make sure the data is correct
      verifyData(data, dataSize, file, fs);
    }
  }

  @Test
  public void testGetCommittedBlockLengthWithException() throws Exception {
    int dataSize = 100;
    final byte[] data = getData(dataSize);

    final FSDataOutputStream stream = fs.create(file, true);
    try {
      stream.write(data);
      stream.hsync();
      assertFalse(fs.isFileClosed(file));

      // write more data without hsync
      stream.write(data);
      stream.flush();

      // close the pipeline and container
      ContainerInfo container = closeLatestContainer();
      // throw exception on first DN getCommittedBlockLength handling
      FaultInjectorImpl injector = new FaultInjectorImpl();
      KeyValueHandler.setInjector(injector);
      StorageContainerException sce = new StorageContainerException(
          "ContainerID " + container.getContainerID() + " does not exist",
          ContainerProtos.Result.CONTAINER_NOT_FOUND);
      injector.setException(sce);

      fs.recoverLease(file);

      String output = xceiverClientLogs.getOutput();
      assertEquals(2, StringUtils.countMatches(output,
          "Executing command cmdType: GetCommittedBlockLength"), output);
      assertEquals(1, StringUtils.countMatches(output,
          "Failed to execute command cmdType: GetCommittedBlockLength"), output);

      // The lease should have been recovered.
      assertTrue(fs.isFileClosed(file), "File should be closed");
      FileStatus fileStatus = fs.getFileStatus(file);
      assertEquals(dataSize * 2, fileStatus.getLen());
    } finally {
      closeIgnoringKeyNotFound(stream);
    }

    // open it again, make sure the data is correct
    verifyData(data, dataSize * 2, file, fs);
  }

  @Test
  @Order(Integer.MAX_VALUE)
  public void testOMConnectionFailure() throws Exception {
    // reduce hadoop RPC retry max attempts
    OzoneConfiguration clientConf = new OzoneConfiguration(conf);
    clientConf.setInt(OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY, 5);
    clientConf.setLong(OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_KEY, 100);
    try (RootedOzoneFileSystem fs = (RootedOzoneFileSystem) FileSystem.get(clientConf)) {
      int dataSize = 100;
      final byte[] data = getData(dataSize);

      final FSDataOutputStream stream = fs.create(file, true);
      OzoneManager om = cluster.getOzoneManager();
      try {
        stream.write(data);
        stream.hsync();
        assertFalse(fs.isFileClosed(file));

        // close OM
        if (om.stop()) {
          om.join();
        }
        assertThrows(ConnectException.class, () -> fs.recoverLease(file));
      } finally {
        try {
          stream.close();
        } catch (Throwable e) {
        }
      }

      om.restart();
      cluster.waitForClusterToBeReady();
      assertTrue(fs.recoverLease(file));
    }
  }

  @Test
  public void testRecoverWrongFile() throws Exception {
    final Path notExistFile = new Path(dir, "file1");

    int dataSize = 100;
    final byte[] data = getData(dataSize);

    final FSDataOutputStream stream = fs.create(file, true);
    try {
      stream.write(data);
      stream.hsync();
      assertFalse(fs.isFileClosed(file));

      assertThrows(FileNotFoundException.class, () -> fs.recoverLease(notExistFile));
    } finally {
      closeIgnoringKeyNotFound(stream);
    }
  }

  @Test
  public void testRecoveryWithoutBlocks() throws Exception {
    final FSDataOutputStream stream = fs.create(file, true);
    try {
      stream.hsync();
      assertFalse(fs.isFileClosed(file));

      int count = 0;
      while (count++ < 15 && !fs.recoverLease(file)) {
        Thread.sleep(1000);
      }
      // The lease should have been recovered.
      assertTrue(fs.isFileClosed(file), "File should be closed");

    } finally {
      closeIgnoringKeyNotFound(stream);
    }
  }

  @Test
  public void testRecoveryWithPartialFilledHsyncBlock() throws Exception {
    int blockSize = (int) cluster.getOzoneManager().getConfiguration().getStorageSize(
        OZONE_SCM_BLOCK_SIZE, OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);
    final byte[] data = getData(blockSize - 1);

    final FSDataOutputStream stream = fs.create(file, true);
    try {
      // Write data into 1st block with total length = blockSize - 1
      stream.write(data);
      stream.hsync();

      StorageContainerManager scm = cluster.getStorageContainerManager();
      // Close container so that new data won't be written into the same block
      // block1 is partially filled
      ContainerInfo container = closeLatestContainer();
      GenericTestUtils.waitFor(() -> {
        try {
          return scm.getPipelineManager().getPipeline(container.getPipelineID()).isClosed();
        } catch (PipelineNotFoundException e) {
          throw new RuntimeException(e);
        }
      }, 200, 30000);

      assertFalse(fs.isFileClosed(file));

      // write data, this data will completely go into block2 even though block1 had 1 space left
      stream.write(data);
      stream.flush();
      // At this point both block1 and block2 has (blockSize - 1) length data

      int count = 0;
      while (count++ < 15 && !fs.recoverLease(file)) {
        Thread.sleep(1000);
      }
      // The lease should have been recovered.
      assertTrue(fs.isFileClosed(file), "File should be closed");

      // A second call to recoverLease should succeed too.
      assertTrue(fs.recoverLease(file));
    } finally {
      closeIgnoringKeyNotFound(stream);
    }

    // open it again, make sure the data is correct
    verifyData(data, (blockSize - 1) * 2, file, fs);
  }

  private ContainerInfo closeLatestContainer() throws IOException, TimeoutException, InterruptedException {
    StorageContainerManager scm = cluster.getStorageContainerManager();
    ContainerInfo container = new LinkedList<>(scm.getContainerManager().getContainers()).getLast();
    OzoneTestUtils.closeContainer(scm, container);
    return container;
  }

  @Test
  public void testRecoveryWithSameBlockCountInOpenFileAndFileTable() throws Exception {
    int blockSize = (int) cluster.getOzoneManager().getConfiguration().getStorageSize(
        OZONE_SCM_BLOCK_SIZE, OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);
    final byte[] data = getData(blockSize / 2 - 1);

    final FSDataOutputStream stream = fs.create(file, true);
    try {
      stream.write(data);
      // block 1 exist in fileTable after hsync with length (blockSize / 2 - 1)
      stream.hsync();
      assertFalse(fs.isFileClosed(file));

      // Write more data without hsync on same block
      // File table block length will be still (blockSize / 2 - 1)
      stream.write(data);
      stream.flush();

      int count = 0;
      // fileTable and openFileTable will have same block count.
      // Both table contains block1
      while (count++ < 15 && !fs.recoverLease(file)) {
        Thread.sleep(1000);
      }
      // The lease should have been recovered.
      assertTrue(fs.isFileClosed(file), "File should be closed");

      // A second call to recoverLease should succeed too.
      assertTrue(fs.recoverLease(file));
    } finally {
      closeIgnoringKeyNotFound(stream);
    }

    // open it again, make sure the data is correct
    verifyData(data, (blockSize / 2 - 1) * 2, file, fs);
  }

  private void verifyData(byte[] data, int dataSize, Path filePath, FileSystem fileSystem) throws IOException {
    try (FSDataInputStream fdis = fileSystem.open(filePath)) {
      int bufferSize = dataSize > data.length ? dataSize / 2 : dataSize;
      while (dataSize > 0) {
        byte[] readData = new byte[bufferSize];
        int readBytes = fdis.read(readData);
        assertEquals(readBytes, bufferSize);
        assertArrayEquals(readData, data);
        dataSize -= bufferSize;
      }
    }
  }

  private byte[] getData(int dataSize) {
    final byte[] data = new byte[dataSize];
    ThreadLocalRandom.current().nextBytes(data);
    return data;
  }
}
