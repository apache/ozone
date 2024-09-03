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
package org.apache.hadoop.fs.ozone;

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
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.utils.FaultInjectorImpl;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.event.Level;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

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
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for recoverLease() API.
 */
@Timeout(300)
@Flaky("HDDS-11323")
public class TestLeaseRecovery {

  private MiniOzoneCluster cluster;
  private OzoneBucket bucket;

  private OzoneClient client;
  private final OzoneConfiguration conf = new OzoneConfiguration();
  private String dir;
  private Path file;

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
    } catch (IOException e) {
      assertEquals(expectedResultCode, ((OMException)e).getResult());
    }
  }

  @BeforeEach
  public void init() throws IOException, InterruptedException,
      TimeoutException {
    final int chunkSize = 16 << 10;
    final int flushSize = 2 * chunkSize;
    final int maxFlushSize = 2 * flushSize;
    final int blockSize = 2 * maxFlushSize;
    final BucketLayout layout = BucketLayout.FILE_SYSTEM_OPTIMIZED;

    conf.setBoolean(OZONE_OM_RATIS_ENABLE_KEY, false);
    conf.setBoolean(OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED, true);
    conf.setBoolean("ozone.client.hbase.enhancements.allowed", true);
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
    bucket = TestDataUtil.createVolumeAndBucket(client, layout);

    GenericTestUtils.setLogLevel(XceiverClientGrpc.getLogger(), Level.DEBUG);

    // Set the fs.defaultFS
    final String rootPath = String.format("%s://%s/", OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    dir = OZONE_ROOT + bucket.getVolumeName() + OZONE_URI_DELIMITER + bucket.getName();
    file = new Path(dir, "file");
  }

  @AfterEach
  public void tearDown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1 << 17, (1 << 17) + 1, (1 << 17) - 1})
  public void testRecovery(int dataSize) throws Exception {
    RootedOzoneFileSystem fs = (RootedOzoneFileSystem)FileSystem.get(conf);

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
    RootedOzoneFileSystem fs = (RootedOzoneFileSystem)FileSystem.get(conf);

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
    // Set the fs.defaultFS
    bucket = TestDataUtil.createVolumeAndBucket(client,
        "vol2", "obs", BucketLayout.OBJECT_STORE);
    final String rootPath = String.format("%s://%s/", OZONE_OFS_URI_SCHEME,
        conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    final String directory = OZONE_ROOT + bucket.getVolumeName() +
        OZONE_URI_DELIMITER + bucket.getName();
    final Path f = new Path(directory, "file");

    RootedOzoneFileSystem fs = (RootedOzoneFileSystem) FileSystem.get(conf);
    assertThrows(IllegalArgumentException.class, () -> fs.recoverLease(f));
  }

  @Test
  public void testFinalizeBlockFailure() throws Exception {
    RootedOzoneFileSystem fs = (RootedOzoneFileSystem)FileSystem.get(conf);
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
      GenericTestUtils.LogCapturer logs =
          GenericTestUtils.LogCapturer.captureLogs(BasicRootedOzoneClientAdapterImpl.LOG);

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
    RootedOzoneFileSystem fs = (RootedOzoneFileSystem)FileSystem.get(conf);
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
      ContainerInfo container = scm.getContainerManager().getContainers().get(0);
      OzoneTestUtils.closeContainer(scm, container);
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
    conf.set(OZONE_CLIENT_READ_TIMEOUT, "2s");
    // set force recovery
    System.setProperty(FORCE_LEASE_RECOVERY_ENV, String.valueOf(forceRecovery));
    RootedOzoneFileSystem fs = (RootedOzoneFileSystem)FileSystem.get(conf);
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
      ContainerInfo container = cluster.getStorageContainerManager().getContainerManager().getContainers().get(0);
      OzoneTestUtils.closeContainer(cluster.getStorageContainerManager(), container);
      // pause getCommittedBlockLength handling on all DNs to make sure all getCommittedBlockLength will time out
      FaultInjectorImpl injector = new FaultInjectorImpl();
      injector.setType(ContainerProtos.Type.GetCommittedBlockLength);
      KeyValueHandler.setInjector(injector);
      GenericTestUtils.LogCapturer logs =
          GenericTestUtils.LogCapturer.captureLogs(XceiverClientGrpc.getLogger());
      if (!forceRecovery) {
        assertThrows(IOException.class, () -> fs.recoverLease(file));
        return;
      } else {
        fs.recoverLease(file);
      }
      assertEquals(3, StringUtils.countMatches(logs.getOutput(),
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
      KeyValueHandler.setInjector(null);
    }

    // open it again, make sure the data is correct
    verifyData(data, dataSize, file, fs);
  }

  @Test
  public void testGetCommittedBlockLengthWithException() throws Exception {
    RootedOzoneFileSystem fs = (RootedOzoneFileSystem)FileSystem.get(conf);
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
      ContainerInfo container = cluster.getStorageContainerManager().getContainerManager().getContainers().get(0);
      OzoneTestUtils.closeContainer(cluster.getStorageContainerManager(), container);
      // throw exception on first DN getCommittedBlockLength handling
      FaultInjectorImpl injector = new FaultInjectorImpl();
      KeyValueHandler.setInjector(injector);
      StorageContainerException sce = new StorageContainerException(
          "ContainerID " + container.getContainerID() + " does not exist",
          ContainerProtos.Result.CONTAINER_NOT_FOUND);
      injector.setException(sce);

      GenericTestUtils.LogCapturer logs =
          GenericTestUtils.LogCapturer.captureLogs(XceiverClientGrpc.getLogger());
      fs.recoverLease(file);

      assertEquals(2, StringUtils.countMatches(logs.getOutput(),
          "Executing command cmdType: GetCommittedBlockLength"));
      assertEquals(1, StringUtils.countMatches(logs.getOutput(),
          "Failed to execute command cmdType: GetCommittedBlockLength"));

      // The lease should have been recovered.
      assertTrue(fs.isFileClosed(file), "File should be closed");
      FileStatus fileStatus = fs.getFileStatus(file);
      assertEquals(dataSize * 2, fileStatus.getLen());
    } finally {
      closeIgnoringKeyNotFound(stream);
      KeyValueHandler.setInjector(null);
    }

    // open it again, make sure the data is correct
    verifyData(data, dataSize * 2, file, fs);
  }

  @Test
  public void testOMConnectionFailure() throws Exception {
    // reduce hadoop RPC retry max attempts
    conf.setInt(OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY, 5);
    conf.setLong(OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_KEY, 100);
    RootedOzoneFileSystem fs = (RootedOzoneFileSystem)FileSystem.get(conf);
    int dataSize = 100;
    final byte[] data = getData(dataSize);

    final FSDataOutputStream stream = fs.create(file, true);
    try {
      stream.write(data);
      stream.hsync();
      assertFalse(fs.isFileClosed(file));

      // close OM
      cluster.getOzoneManager().stop();
      assertThrows(ConnectException.class, () -> fs.recoverLease(file));
    } finally {
      try {
        stream.close();
      } catch (Throwable e) {
      }
      cluster.getOzoneManager().restart();
      cluster.waitForClusterToBeReady();
      assertTrue(fs.recoverLease(file));
    }
  }

  @Test
  public void testRecoverWrongFile() throws Exception {
    final Path notExistFile = new Path(dir, "file1");

    RootedOzoneFileSystem fs = (RootedOzoneFileSystem)FileSystem.get(conf);
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
    RootedOzoneFileSystem fs = (RootedOzoneFileSystem)FileSystem.get(conf);

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
    RootedOzoneFileSystem fs = (RootedOzoneFileSystem)FileSystem.get(conf);
    int blockSize = (int) cluster.getOzoneManager().getConfiguration().getStorageSize(
        OZONE_SCM_BLOCK_SIZE, OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);
    final byte[] data = getData(blockSize - 1);

    final FSDataOutputStream stream = fs.create(file, true);
    try {
      // Write data into 1st block with total length = blockSize - 1
      stream.write(data);
      stream.hsync();

      StorageContainerManager scm = cluster.getStorageContainerManager();
      ContainerInfo container = scm.getContainerManager().getContainers().get(0);
      // Close container so that new data won't be written into the same block
      // block1 is partially filled
      OzoneTestUtils.closeContainer(scm, container);
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

  @Test
  public void testRecoveryWithSameBlockCountInOpenFileAndFileTable() throws Exception {
    RootedOzoneFileSystem fs = (RootedOzoneFileSystem)FileSystem.get(conf);
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

  private void verifyData(byte[] data, int dataSize, Path filePath, RootedOzoneFileSystem fs) throws IOException {
    try (FSDataInputStream fdis = fs.open(filePath)) {
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
