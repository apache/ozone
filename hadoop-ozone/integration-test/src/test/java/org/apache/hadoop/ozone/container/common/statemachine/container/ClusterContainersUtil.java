package org.apache.hadoop.ozone.container.common.statemachine.container;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.ratis.io.CorruptedFileException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Utility method to manipulate/inspect container data on disk in a mini cluster.
 */
public final class ClusterContainersUtil {
  private ClusterContainersUtil() {
  }

  /**
   *
   *
   * @param cluster a mini ozone cluster object.
   * @param container a container object.
   * @param key an OzoneKey object.
   * @return the location of the chunk file.
   * @throws IOException
   */
  public static File getChunksLocationPath(MiniOzoneCluster cluster, Container container, OzoneKey key)
      throws IOException {
    long containerID = ((OzoneKeyDetails) key).getOzoneKeyLocations().get(0)
        .getContainerID();
    long localID = ((OzoneKeyDetails) key).getOzoneKeyLocations().get(0)
        .getLocalID();
    // From the containerData, get the block iterator for all the blocks in
    // the container.
    KeyValueContainerData containerData =
        (KeyValueContainerData) container.getContainerData();
    try (DBHandle db = BlockUtils.getDB(containerData, cluster.getConf());
         BlockIterator<BlockData> keyValueBlockIterator =
             db.getStore().getBlockIterator(containerID)) {
      // Find the block corresponding to the key we put. We use the localID of
      // the BlockData to identify out key.
      BlockData blockData = null;
      while (keyValueBlockIterator.hasNext()) {
        blockData = keyValueBlockIterator.nextBlock();
        if (blockData.getBlockID().getLocalID() == localID) {
          break;
        }
      }
      assertNotNull(blockData, "Block not found");

      // Get the location of the chunk file
      String containreBaseDir =
          container.getContainerData().getVolume().getHddsRootDir().getPath();
      File chunksLocationPath = KeyValueContainerLocationUtil
          .getChunksLocationPath(containreBaseDir, cluster.getClusterId(), containerID);
      return chunksLocationPath;
    }
  }

  /**
   * Corrupt the chunk backing the key in a mini cluster.
   * @param cluster a mini ozone cluster object.
   * @param container a container object.
   * @param key an OzoneKey object.
   * @throws IOException
   */
  public static void corruptData(MiniOzoneCluster cluster, Container container, OzoneKey key)
      throws IOException {
    File chunksLocationPath = getChunksLocationPath(cluster, container, key);
    byte[] corruptData = "corrupted data".getBytes(UTF_8);
    // Corrupt the contents of chunk files
    for (File file : FileUtils.listFiles(chunksLocationPath, null, false)) {
      FileUtils.writeByteArrayToFile(file, corruptData);
    }
  }

  /**
   * Inspect and verify if chunk backing the key in a mini cluster is the same as the string.
   * @param cluster a mini ozone cluster object.
   * @param container a container object.
   * @param key an OzoneKey object.
   * @throws IOException
   */
  public static void verifyOnDiskData(MiniOzoneCluster cluster, Container container, OzoneKey key, String data)
      throws IOException {
    File chunksLocationPath = getChunksLocationPath(cluster, container, key);
    for (File file : FileUtils.listFiles(chunksLocationPath, null, false)) {
      String chunkOnDisk = FileUtils.readFileToString(file, Charset.defaultCharset());
      if (!data.equals(chunkOnDisk)) {
        throw new CorruptedFileException(file, " does not match the source.");
      }
    }
  }

  /**
   * Return the first container object in a mini cluster specified by its ID.
   * @param cluster a mini ozone cluster object.
   * @param containerID a long variable representing cluater ID.
   * @return
   */
  public static Container getContainerByID(MiniOzoneCluster cluster, long containerID) {
    // Get the container by traversing the datanodes. Atleast one of the
    // datanode must have this container.
    Container container = null;
    for (HddsDatanodeService hddsDatanode : cluster.getHddsDatanodes()) {
      container = hddsDatanode.getDatanodeStateMachine().getContainer()
          .getContainerSet().getContainer(containerID);
      if (container != null) {
        break;
      }
    }
    return container;
  }
}
