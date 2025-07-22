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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.cli.ScmOption;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
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

  @CommandLine.Option(names = {"--all-results"},
      description = "Print results for all passing and failing keys")
  private boolean allResults;

  @CommandLine.ArgGroup(exclusive = false, multiplicity = "1")
  private Verification verification;

  @CommandLine.Option(names = {"--container-cache-size"},
      description = "Size (in number of containers) of the in-memory cache for container state verification " +
          "'--container-state'. Default is 1 million containers (which takes around 43MB). " +
          "Value must be greater than zero, otherwise the default of 1 million is considered. " +
          "Note: This option is ignored if '--container-state' option is not used.",
      defaultValue = "1000000")
  private long containerCacheSize;

  private List<ReplicaVerifier> replicaVerifiers;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address) throws IOException {
    replicaVerifiers = new ArrayList<>();

    if (verification.doExecuteChecksums) {
      replicaVerifiers.add(new ChecksumVerifier(getConf()));
    }

    if (verification.doExecuteBlockExistence) {
      replicaVerifiers.add(new BlockExistenceVerifier(getConf()));
    }
    if (verification.doExecuteReplicaState) {
      replicaVerifiers.add(new ContainerStateVerifier(getConf(), containerCacheSize));
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

    ObjectNode root = JsonUtils.createObjectNode(null);
    ArrayNode keysArray = root.putArray("keys");

    AtomicBoolean allKeysPassed = new AtomicBoolean(true);

    if (!keyName.isEmpty()) {
      processKey(ozoneClient, volumeName, bucketName, keyName, keysArray, allKeysPassed);
    } else if (!bucketName.isEmpty()) {
      OzoneVolume volume = objectStore.getVolume(volumeName);
      OzoneBucket bucket = volume.getBucket(bucketName);
      checkBucket(ozoneClient, bucket, keysArray, allKeysPassed);
    } else if (!volumeName.isEmpty()) {
      OzoneVolume volume = objectStore.getVolume(volumeName);
      checkVolume(ozoneClient, volume, keysArray, allKeysPassed);
    } else {
      for (Iterator<? extends OzoneVolume> it = objectStore.listVolumes(null); it.hasNext();) {
        checkVolume(ozoneClient, it.next(), keysArray, allKeysPassed);
      }
    }
    root.put("pass", allKeysPassed.get());
    System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(root));
  }

  void checkVolume(OzoneClient ozoneClient, OzoneVolume volume, ArrayNode keysArray, AtomicBoolean allKeysPassed)
      throws IOException {
    for (Iterator<? extends OzoneBucket> it = volume.listBuckets(null); it.hasNext();) {
      OzoneBucket bucket = it.next();
      checkBucket(ozoneClient, bucket, keysArray, allKeysPassed);
    }
  }

  void checkBucket(OzoneClient ozoneClient, OzoneBucket bucket, ArrayNode keysArray, AtomicBoolean allKeysPassed)
      throws IOException {
    for (Iterator<? extends OzoneKey> it = bucket.listKeys(null); it.hasNext();) {
      OzoneKey key = it.next();
      // TODO: Remove this check once HDDS-12094 is fixed
      if (!key.getName().endsWith("/")) {
        processKey(ozoneClient, key.getVolumeName(), key.getBucketName(), key.getName(), keysArray, allKeysPassed);
      }
    }
  }

  void processKey(OzoneClient ozoneClient, String volumeName, String bucketName, String keyName,
      ArrayNode keysArray, AtomicBoolean allKeysPassed) throws IOException {
    OmKeyInfo keyInfo = ozoneClient.getProxy().getKeyInfo(
        volumeName, bucketName, keyName, false);

    ObjectNode keyNode = JsonUtils.createObjectNode(null);
    keyNode.put("volumeName", volumeName);
    keyNode.put("bucketName", bucketName);
    keyNode.put("name", keyName);

    ArrayNode blocksArray = keyNode.putArray("blocks");
    boolean keyPass = true;

    for (OmKeyLocationInfo keyLocation : keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly()) {
      long containerID = keyLocation.getContainerID();
      long localID = keyLocation.getLocalID();

      ObjectNode blockNode = blocksArray.addObject();
      blockNode.put("containerID", containerID);
      blockNode.put("blockID", localID);

      ArrayNode replicasArray = blockNode.putArray("replicas");
      boolean blockPass = true;

      for (DatanodeDetails datanode : keyLocation.getPipeline().getNodes()) {
        ObjectNode replicaNode = replicasArray.addObject();

        ObjectNode datanodeNode = replicaNode.putObject("datanode");
        datanodeNode.put("uuid", datanode.getUuidString());
        datanodeNode.put("hostname", datanode.getHostName());

        ArrayNode checksArray = replicaNode.putArray("checks");
        boolean replicaPass = true;
        int replicaIndex = keyLocation.getPipeline().getReplicaIndex(datanode);

        for (ReplicaVerifier verifier : replicaVerifiers) {
          BlockVerificationResult result = verifier.verifyBlock(datanode, keyLocation);
          ObjectNode checkNode = checksArray.addObject();
          checkNode.put("type", verifier.getType());
          checkNode.put("completed", result.isCompleted());
          checkNode.put("pass", result.passed());

          ArrayNode failuresArray = checkNode.putArray("failures");
          for (String failure : result.getFailures()) {
            failuresArray.addObject().put("message", failure);
          }
          replicaNode.put("replicaIndex", replicaIndex);

          if (!result.passed()) {
            replicaPass = false;
          }
        }

        if (!replicaPass) {
          blockPass = false;
        }
      }

      if (!blockPass) {
        keyPass = false;
      }
    }

    keyNode.put("pass", keyPass);
    if (!keyPass) {
      allKeysPassed.set(false);
    }

    if (!keyPass || allResults) {
      keysArray.add(keyNode);
    }
  }

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

    @CommandLine.Option(names = "--container-state",
        description = "Check the container and replica states. " +
            "Containers in [DELETING, DELETED] states, or " +
            "it's replicas in [DELETED, UNHEALTHY, INVALID] states fail the check.",
        defaultValue = "false")
    private boolean doExecuteReplicaState;

  }
}
