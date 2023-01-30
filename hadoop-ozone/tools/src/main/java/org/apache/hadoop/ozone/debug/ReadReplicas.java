/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.debug;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.keys.KeyHandler;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.jetbrains.annotations.NotNull;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Class that downloads every replica for all the blocks associated with a
 * given key. It also generates a manifest file with information about the
 * downloaded replicas.
 */
@CommandLine.Command(name = "read-replicas",
    description = "Reads every replica for all the blocks associated with a " +
        "given key.")
@MetaInfServices(SubcommandWithParent.class)
public class ReadReplicas extends KeyHandler implements SubcommandWithParent {

  @CommandLine.Option(names = {"--outputDir", "-o"},
      description = "Destination where the directory will be created" +
          " for the downloaded replicas and the manifest file.",
      defaultValue = "/opt/hadoop")
  private String outputDir;

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

  private ClientProtocol clientProtocol;
  private ClientProtocol clientProtocolWithoutChecksum;

  @Override
  public Class<?> getParentType() {
    return OzoneDebug.class;
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException, OzoneClientException {

    boolean isChecksumVerifyEnabled
        = getConf().getBoolean("ozone.client.verify.checksum", true);
    OzoneConfiguration configuration = new OzoneConfiguration(getConf());
    configuration.setBoolean("ozone.client.verify.checksum",
        !isChecksumVerifyEnabled);

    if (isChecksumVerifyEnabled) {
      clientProtocol = client.getObjectStore().getClientProxy();
      clientProtocolWithoutChecksum = new RpcClient(configuration, null);
    } else {
      clientProtocol = new RpcClient(configuration, null);
      clientProtocolWithoutChecksum = client.getObjectStore().getClientProxy();
    }

    address.ensureKeyAddress();
    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    String keyName = address.getKeyName();

    String directoryName = createDirectory(volumeName, bucketName, keyName);

    OzoneKeyDetails keyInfoDetails
        = clientProtocol.getKeyDetails(volumeName, bucketName, keyName);

    Map<OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream>> replicas
        = clientProtocol.getKeysEveryReplicas(volumeName, bucketName, keyName);

    Map<OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream>>
        replicasWithoutChecksum = clientProtocolWithoutChecksum
        .getKeysEveryReplicas(volumeName, bucketName, keyName);

    JsonObject result = new JsonObject();
    result.addProperty(JSON_PROPERTY_FILE_NAME,
        volumeName + "/" + bucketName + "/" + keyName);
    result.addProperty(JSON_PROPERTY_FILE_SIZE, keyInfoDetails.getDataSize());

    JsonArray blocks = new JsonArray();
    downloadReplicasAndCreateManifest(keyName, replicas,
        replicasWithoutChecksum, directoryName, blocks);
    result.add(JSON_PROPERTY_FILE_BLOCKS, blocks);

    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    String prettyJson = gson.toJson(result);

    String manifestFileName = keyName + "_manifest";
    System.out.println("Writing manifest file : " + manifestFileName);
    File manifestFile
        = new File(outputDir + "/" + directoryName + "/" + manifestFileName);
    Files.write(manifestFile.toPath(),
        prettyJson.getBytes(StandardCharsets.UTF_8));
  }

  private void downloadReplicasAndCreateManifest(
      String keyName,
      Map<OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream>> replicas,
      Map<OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream>>
          replicasWithoutChecksum,
      String directoryName, JsonArray blocks) throws IOException {
    int blockIndex = 0;

    for (Map.Entry<OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream>>
        block : replicas.entrySet()) {
      JsonObject blockJson = new JsonObject();
      JsonArray replicasJson = new JsonArray();

      blockIndex += 1;
      blockJson.addProperty(JSON_PROPERTY_BLOCK_INDEX, blockIndex);
      blockJson.addProperty(JSON_PROPERTY_BLOCK_CONTAINERID,
          block.getKey().getContainerID());
      blockJson.addProperty(JSON_PROPERTY_BLOCK_LOCALID,
          block.getKey().getLocalID());
      blockJson.addProperty(JSON_PROPERTY_BLOCK_LENGTH,
          block.getKey().getLength());
      blockJson.addProperty(JSON_PROPERTY_BLOCK_OFFSET,
          block.getKey().getOffset());

      for (Map.Entry<DatanodeDetails, OzoneInputStream>
          replica : block.getValue().entrySet()) {
        JsonObject replicaJson = new JsonObject();

        replicaJson.addProperty(JSON_PROPERTY_REPLICA_HOSTNAME,
            replica.getKey().getHostName());
        replicaJson.addProperty(JSON_PROPERTY_REPLICA_UUID,
            replica.getKey().getUuidString());

        OzoneInputStream is = replica.getValue();
        String fileName = keyName + "_block" + blockIndex + "_" +
            replica.getKey().getHostName();
        System.out.println("Writing : " + fileName);
        File replicaFile
            = new File(outputDir + "/" + directoryName + "/" + fileName);

        try {
          Files.copy(is, replicaFile.toPath(),
              StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
          Throwable cause = e.getCause();
          replicaJson.addProperty(JSON_PROPERTY_REPLICA_EXCEPTION,
              e.getMessage());
          if (cause instanceof OzoneChecksumException) {
            BlockID blockID = block.getKey().getBlockID();
            String datanodeUUID = replica.getKey().getUuidString();
            is = getInputStreamWithoutChecksum(replicasWithoutChecksum,
                datanodeUUID, blockID);
            Files.copy(is, replicaFile.toPath(),
                StandardCopyOption.REPLACE_EXISTING);
          } else if (cause instanceof StatusRuntimeException) {
            break;
          }
        } finally {
          is.close();
        }
        replicasJson.add(replicaJson);
      }
      blockJson.add(JSON_PROPERTY_BLOCK_REPLICAS, replicasJson);
      blocks.add(blockJson);
    }
  }

  private OzoneInputStream getInputStreamWithoutChecksum(
      Map<OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream>>
          replicasWithoutChecksum, String datanodeUUID, BlockID blockID) {
    OzoneInputStream is = new OzoneInputStream();
    for (Map.Entry<OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream>>
        block : replicasWithoutChecksum.entrySet()) {
      if (block.getKey().getBlockID().equals(blockID)) {
        for (Map.Entry<DatanodeDetails, OzoneInputStream>
            replica : block.getValue().entrySet()) {
          if (replica.getKey().getUuidString().equals(datanodeUUID)) {
            is = replica.getValue();
          }
        }
      }
    }
    return is;
  }

  @NotNull
  private String createDirectory(String volumeName, String bucketName,
                                 String keyName) throws IOException {
    String fileSuffix
        = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    String directoryName = volumeName + "_" + bucketName + "_" + keyName +
        "_" + fileSuffix;
    System.out.println("Creating directory : " + directoryName);
    File dir = new File(outputDir + "/" + directoryName);
    if (!dir.exists()) {
      if (dir.mkdir()) {
        System.out.println("Successfully created!");
      } else {
        throw new IOException(String.format(
            "Failed to create directory %s.", dir));
      }
    }
    return directoryName;
  }
}
