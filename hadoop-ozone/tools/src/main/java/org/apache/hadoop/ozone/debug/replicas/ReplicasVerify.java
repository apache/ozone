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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.cli.ScmOption;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.Shell;
import picocli.CommandLine;

/**
 * Verify replicas command.
 */

@CommandLine.Command(
    name = "verify",
    description = "Run checks to verify data across replicas. By default prints only the keys with failed checks.")
public class ReplicasVerify extends Handler {
  @CommandLine.Mixin
  private ScmOption scmOption;

  @CommandLine.Parameters(arity = "1",
      description = Shell.OZONE_URI_DESCRIPTION)
  private String uri;

  @CommandLine.Option(names = {"-o", "--output-dir"},
      description = "Destination directory to save the generated output.",
      required = true)
  private String outputDir;

  @CommandLine.Option(names = {"--all"},
      description = "Print results for all passing and failing keys")
  private boolean allKeys;

  @CommandLine.ArgGroup(exclusive = false, multiplicity = "1")
  private Verification verification;

  static class Verification {
    @CommandLine.Option(names = "--checksums",
        description = "Do client side data checksum validation of all replicas.",
        // value will be true only if the "--checksums" option was specified on the CLI
        defaultValue = "false")
    private boolean doExecuteChecksums;

    @CommandLine.Option(names = "--block-existence",
        description = "Check for block existence on datanodes.",
        defaultValue = "false")
    private boolean doExecuteBlockExistence;

  }
  private List<ReplicaVerifier> replicaVerifiers;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address) throws IOException {
    replicaVerifiers = new ArrayList<>();

    if (verification.doExecuteChecksums) {
      replicaVerifiers.add(new Checksums());
    }

    if (verification.doExecuteBlockExistence) {
      replicaVerifiers.add(new BlockExistenceVerifier(getConf()));
    }

    findCandidateKeys(client, address);
  }

  @Override
  protected OzoneAddress getAddress() throws OzoneClientException {
    return new OzoneAddress(uri);
  }

  void findCandidateKeys(OzoneClient ozoneClient, OzoneAddress address) throws IOException {
    ObjectStore objectStore = ozoneClient.getObjectStore();
    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    String keyName = address.getKeyName();

    ObjectMapper mapper = new ObjectMapper();
    ObjectNode root = mapper.createObjectNode();
    ArrayNode keysArray = mapper.createArrayNode();

    if (!keyName.isEmpty()) {
      OzoneKeyDetails keyDetails = ozoneClient.getProxy().getKeyDetails(volumeName, bucketName, keyName);
      processKey(ozoneClient, keyDetails, keysArray);
    } else if (!bucketName.isEmpty()) {
      OzoneVolume volume = objectStore.getVolume(volumeName);
      OzoneBucket bucket = volume.getBucket(bucketName);
      checkBucket(ozoneClient, bucket, keysArray);
    } else if (!volumeName.isEmpty()) {
      OzoneVolume volume = objectStore.getVolume(volumeName);
      checkVolume(ozoneClient, volume, keysArray);
    } else {
      for (Iterator<? extends OzoneVolume> it = objectStore.listVolumes(null); it.hasNext();) {
        checkVolume(ozoneClient, it.next(), keysArray);
      }
    }

    root.set("keys", keysArray);
    System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root));
  }

  void checkVolume(OzoneClient ozoneClient, OzoneVolume volume, ArrayNode keysArray) throws IOException {
    for (Iterator<? extends OzoneBucket> it = volume.listBuckets(null); it.hasNext();) {
      OzoneBucket bucket = it.next();
      checkBucket(ozoneClient, bucket, keysArray);
    }
  }

  void checkBucket(OzoneClient ozoneClient, OzoneBucket bucket, ArrayNode keysArray) throws IOException {
    for (Iterator<? extends OzoneKey> it = bucket.listKeys(null); it.hasNext();) {
      OzoneKey key = it.next();
      // TODO: Remove this check once HDDS-12094 is fixed
      if (!key.getName().endsWith("/")) {
        processKey(ozoneClient, bucket.getKey(key.getName()), keysArray);
      }
    }
  }

  void processKey(OzoneClient ozoneClient, OzoneKeyDetails keyDetails, ArrayNode keysArray) {
    ObjectMapper mapper = new ObjectMapper();

    String volumeName = keyDetails.getVolumeName();
    String bucketName = keyDetails.getBucketName();
    String keyName = keyDetails.getName();

    ObjectNode keyNode = JsonUtils.createObjectNode(null);
    keyNode.put("volumeName", volumeName);
    keyNode.put("bucketName", bucketName);
    keyNode.put("name", keyName);

    ArrayNode blocksArray = mapper.createArrayNode();
    boolean keyPass = true; //to check if all checks are passed

    try {
      ClientProtocol clientProtocol = ozoneClient.getObjectStore().getClientProxy();
      Map<OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream>> replicas =
          clientProtocol.getKeysEveryReplicas(volumeName, bucketName, keyName);

      for (Map.Entry<OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream>> block : replicas.entrySet()) {
        OmKeyLocationInfo keyLocation = block.getKey();
        long containerID = keyLocation.getContainerID();
        long localID = keyLocation.getLocalID();

        ObjectNode blockNode = JsonUtils.createObjectNode(null);
        blockNode.put("containerID", containerID);
        blockNode.put("localID", localID);

        ArrayNode replicasArray = mapper.createArrayNode();
        boolean blockPass = !block.getValue().isEmpty(); // If no replicas, the block automatically fails

        for (Map.Entry<DatanodeDetails, OzoneInputStream> replica : block.getValue().entrySet()) {
          DatanodeDetails datanode = replica.getKey();

          ObjectNode datanodeNode = JsonUtils.createObjectNode(null);
          datanodeNode.put("uuid", datanode.getUuidString());
          datanodeNode.put("hostname", datanode.getHostName());


          ArrayNode checksArray = mapper.createArrayNode();
          boolean replicaPass = true;

          ObjectMapper replicaVerifierNode = new ObjectMapper();
          for (ReplicaVerifier verifier : replicaVerifiers) {
            BlockVerificationResult result = verifier.verifyBlock(replica.getKey(), replica.getValue(), keyLocation);
            ObjectNode checksNode = result.toJson(replicaVerifierNode);
            checksArray.add(checksNode);

            if (!result.isPass()) {
              replicaPass = false;
            }
          }

          ObjectNode replicaNode = mapper.createObjectNode();
          replicaNode.set("datanode", datanodeNode);
          replicaNode.set("checks", checksArray);
          //replicaNode.put("pass", replicaPass);  // ← include pass flag for replica if needed

          replicasArray.add(replicaNode);

          if (!replicaPass) {
            blockPass = false;
          }
        }
        blockNode.set("replicas", replicasArray);
       // blockNode.put("pass", blockPass);  // ← pass flag per block

        blocksArray.add(blockNode);

        if (!blockPass) {
          keyPass = false;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Error retrieving replicas for key: " + keyName);
    }

    if (!keyPass || allKeys) {
      keyNode.set("blocks", blocksArray);
      keyNode.put("pass", keyPass);
      keysArray.add(keyNode);
    }
  }
}
