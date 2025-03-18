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

package org.apache.hadoop.ozone.debug.replicas;

import static java.util.Collections.emptyMap;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.slf4j.Logger;

/**
 * Class that downloads every replica for all the blocks associated with a
 * given key. It also generates a manifest file with information about the
 * downloaded replicas.
 */
public class Checksums implements ReplicaVerifier {

  private static final String JSON_PROPERTY_FILE_NAME = "filename";
  private static final String JSON_PROPERTY_FILE_SIZE = "datasize";
  private static final String JSON_PROPERTY_FILE_BLOCKS = "blocks";
  private static final String JSON_PROPERTY_BLOCK_INDEX = "blockIndex";
  private static final String JSON_PROPERTY_BLOCK_CONTAINERID = "containerId";
  private static final String JSON_PROPERTY_BLOCK_LOCALID = "localId";
  private static final String JSON_PROPERTY_BLOCK_LENGTH = "length";
  private static final String JSON_PROPERTY_BLOCK_OFFSET = "offset";
  private static final String JSON_PROPERTY_BLOCK_REPLICAS = "replicas";
  private static final String JSON_PROPERTY_REPLICA_HOSTNAME = "hostname";
  private static final String JSON_PROPERTY_REPLICA_UUID = "uuid";
  private static final String JSON_PROPERTY_REPLICA_EXCEPTION = "exception";


  private String outputDir;
  private RpcClient rpcClient = null;
  private OzoneClient client;
  private Logger log;
  private OzoneConfiguration ozoneConfiguration;

  public Checksums(OzoneClient client, String outputDir, Logger log, OzoneConfiguration conf) {
    this.client = client;
    this.outputDir = outputDir;
    this.log = log;
    this.ozoneConfiguration = conf;
  }

  private void downloadReplicasAndCreateManifest(
      String keyName,
      Map<OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream>> replicas,
      Map<OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream>>
          replicasWithoutChecksum,
      File dir, ArrayNode blocks) throws IOException {
    int blockIndex = 0;

    for (Map.Entry<OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream>>
        block : replicas.entrySet()) {
      ObjectNode blockJson = JsonUtils.createObjectNode(null);
      ArrayNode replicasJson = JsonUtils.createArrayNode();

      blockIndex += 1;
      OmKeyLocationInfo locationInfo = block.getKey();
      blockJson.put(JSON_PROPERTY_BLOCK_INDEX, blockIndex);
      blockJson.put(JSON_PROPERTY_BLOCK_CONTAINERID,
          locationInfo.getContainerID());
      blockJson.put(JSON_PROPERTY_BLOCK_LOCALID, locationInfo.getLocalID());
      blockJson.put(JSON_PROPERTY_BLOCK_LENGTH, locationInfo.getLength());
      blockJson.put(JSON_PROPERTY_BLOCK_OFFSET, locationInfo.getOffset());

      BlockID blockID = locationInfo.getBlockID();
      Map<DatanodeDetails, OzoneInputStream> blockReplicasWithoutChecksum =
          replicasOf(blockID, replicasWithoutChecksum);

      for (Map.Entry<DatanodeDetails, OzoneInputStream>
          replica : block.getValue().entrySet()) {
        DatanodeDetails datanode = replica.getKey();

        ObjectNode replicaJson = JsonUtils.createObjectNode(null);

        replicaJson.put(JSON_PROPERTY_REPLICA_HOSTNAME, datanode.getHostName());
        replicaJson.put(JSON_PROPERTY_REPLICA_UUID, datanode.getUuidString());

        String fileName = keyName + "_block" + blockIndex + "_" +
            datanode.getHostName();
        Path path = new File(dir, fileName).toPath();

        try (InputStream is = replica.getValue()) {
          Files.copy(is, path, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
          Throwable cause = e.getCause();
          replicaJson.put(JSON_PROPERTY_REPLICA_EXCEPTION, e.getMessage());
          if (cause instanceof OzoneChecksumException) {
            try (InputStream is = getReplica(
                blockReplicasWithoutChecksum, datanode)) {
              Files.copy(is, path, StandardCopyOption.REPLACE_EXISTING);
            }
          }
        }
        replicasJson.add(replicaJson);
      }
      blockJson.set(JSON_PROPERTY_BLOCK_REPLICAS, replicasJson);
      blocks.add(blockJson);

      IOUtils.close(log, blockReplicasWithoutChecksum.values());
    }
  }

  private Map<DatanodeDetails, OzoneInputStream> replicasOf(BlockID blockID,
      Map<OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream>> replicas) {
    for (Map.Entry<OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream>>
        block : replicas.entrySet()) {
      if (block.getKey().getBlockID().equals(blockID)) {
        return block.getValue();
      }
    }
    return emptyMap();
  }

  private InputStream getReplica(
      Map<DatanodeDetails, OzoneInputStream> replicas, DatanodeDetails datanode
  ) {
    InputStream input = replicas.remove(datanode);
    return input != null ? input : new ByteArrayInputStream(new byte[0]);
  }

  @Nonnull
  private File createDirectory(String volumeName, String bucketName,
                                 String keyName) throws IOException {
    String fileSuffix
        = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    String directoryName = volumeName + "_" + bucketName + "_" + keyName +
        "_" + fileSuffix;
    System.out.println("Creating directory : " + directoryName);
    File dir = new File(outputDir, directoryName);
    if (!dir.exists()) {
      if (dir.mkdirs()) {
        System.out.println("Successfully created!");
      } else {
        throw new IOException(String.format(
            "Failed to create directory %s.", dir));
      }
    }
    return dir;
  }

  @Override
  public void verifyKey(OzoneKeyDetails keyDetails) {
    String volumeName = keyDetails.getVolumeName();
    String bucketName = keyDetails.getBucketName();
    String keyName = keyDetails.getName();
    System.out.println("Processing key : " + volumeName + "/" + bucketName + "/" + keyName);
    boolean isChecksumVerifyEnabled = ozoneConfiguration.getBoolean("ozone.client.verify.checksum", true);
    RpcClient newClient = null;
    try {
      OzoneConfiguration configuration = new OzoneConfiguration(ozoneConfiguration);
      configuration.setBoolean("ozone.client.verify.checksum", !isChecksumVerifyEnabled);
      newClient = getClient(isChecksumVerifyEnabled);
      ClientProtocol noChecksumClient;
      ClientProtocol checksumClient;
      if (isChecksumVerifyEnabled) {
        checksumClient = client.getObjectStore().getClientProxy();
        noChecksumClient = newClient;
      } else {
        checksumClient = newClient;
        noChecksumClient = client.getObjectStore().getClientProxy();
      }

      // Multilevel keys will have a '/' in their names. This interferes with
      // directory and file creation process. Flatten the keys to fix this.
      String sanitizedKeyName = keyName.replace("/", "_");

      File dir = createDirectory(volumeName, bucketName, sanitizedKeyName);
      OzoneKeyDetails keyInfoDetails = checksumClient.getKeyDetails(volumeName, bucketName, keyName);
      Map<OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream>> replicas =
          checksumClient.getKeysEveryReplicas(volumeName, bucketName, keyName);
      Map<OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream>> replicasWithoutChecksum =
          noChecksumClient.getKeysEveryReplicas(volumeName, bucketName, keyName);

      ObjectNode result = JsonUtils.createObjectNode(null);
      result.put(JSON_PROPERTY_FILE_NAME, volumeName + "/" + bucketName + "/" + keyName);
      result.put(JSON_PROPERTY_FILE_SIZE, keyInfoDetails.getDataSize());

      ArrayNode blocks = JsonUtils.createArrayNode();
      downloadReplicasAndCreateManifest(sanitizedKeyName, replicas, replicasWithoutChecksum, dir, blocks);
      result.set(JSON_PROPERTY_FILE_BLOCKS, blocks);

      String prettyJson = JsonUtils.toJsonStringWithDefaultPrettyPrinter(result);

      String manifestFileName = sanitizedKeyName + "_manifest";
      File manifestFile = new File(dir, manifestFileName);
      Files.write(manifestFile.toPath(), prettyJson.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private RpcClient getClient(boolean isChecksumVerifyEnabled) throws IOException {
    if (rpcClient != null) {
      return rpcClient;
    }

    OzoneConfiguration configuration = new OzoneConfiguration(ozoneConfiguration);
    configuration.setBoolean("ozone.client.verify.checksum", !isChecksumVerifyEnabled);
    rpcClient = new RpcClient(configuration, null);
    return rpcClient;
  }
}
