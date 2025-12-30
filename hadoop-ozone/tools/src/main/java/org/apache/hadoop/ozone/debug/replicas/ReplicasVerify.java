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

import static org.apache.hadoop.ozone.conf.OzoneServiceConfig.DEFAULT_SHUTDOWN_HOOK_PRIORITY;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
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
import org.apache.hadoop.ozone.shell.ShellReplicationOptions;
import org.apache.hadoop.ozone.util.ShutdownHookManager;
import picocli.CommandLine;

/**
 * Verify replicas command.
 */

@CommandLine.Command(
    name = "verify",
    description = "Run checks to verify data across replicas. By default prints only the keys with failed checks. " +
        "Optionally you can filter keys by replication type (RATIS, EC) and factor.")
public class ReplicasVerify extends Handler {
  @CommandLine.Mixin
  private ScmOption scmOption;

  @CommandLine.Mixin
  private ShellReplicationOptions replication;

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

  private static final String DURATION_FORMAT = "HH:mm:ss,SSS";
  private long startTime;
  private long endTime;
  private String verificationScope;
  private final List<String> verificationTypes = new ArrayList<>();
  private final AtomicInteger volumesProcessed = new AtomicInteger(0);
  private final AtomicInteger bucketsProcessed = new AtomicInteger(0);
  private final AtomicInteger keysProcessed = new AtomicInteger(0);
  private final AtomicInteger keysPassed = new AtomicInteger(0);
  private final AtomicInteger keysFailed = new AtomicInteger(0);
  private final Map<String, AtomicInteger> failuresByType = new ConcurrentHashMap<>();
  private volatile Throwable exception;

  private void addVerifier(boolean condition, Supplier<ReplicaVerifier> verifierSupplier) {
    if (condition) {
      ReplicaVerifier verifier = verifierSupplier.get();
      replicaVerifiers.add(verifier);
      String verifierType = verifier.getType();
      verificationTypes.add(verifierType);
      failuresByType.put(verifierType, new AtomicInteger(0));
    }
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address) throws IOException {
    startTime = System.nanoTime();

    if (!address.getKeyName().isEmpty()) {
      verificationScope = "Key";
    } else if (!address.getBucketName().isEmpty()) {
      verificationScope = "Bucket";
    } else if (!address.getVolumeName().isEmpty()) {
      verificationScope = "Volume";
    } else {
      verificationScope = "All Volumes";
    }

    replicaVerifiers = new ArrayList<>();

    addVerifier(verification.doExecuteChecksums, () -> {
      try {
        return new ChecksumVerifier(getConf());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    addVerifier(verification.doExecuteBlockExistence, () -> {
      try {
        return new BlockExistenceVerifier(getConf());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    addVerifier(verification.doExecuteReplicaState, () -> {
      try {
        return new ContainerStateVerifier(getConf(), containerCacheSize);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    // Add shutdown hook to ensure summary is printed even if interrupted
    addShutdownHook();

    try {
      findCandidateKeys(client, address);
    } catch (Exception e) {
      exception = e;
      throw e;
    } finally {
      endTime = System.nanoTime();
    }
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
    volumesProcessed.incrementAndGet();
    for (Iterator<? extends OzoneBucket> it = volume.listBuckets(null); it.hasNext();) {
      OzoneBucket bucket = it.next();
      checkBucket(ozoneClient, bucket, keysArray, allKeysPassed);
    }
  }

  void checkBucket(OzoneClient ozoneClient, OzoneBucket bucket, ArrayNode keysArray, AtomicBoolean allKeysPassed)
      throws IOException {
    bucketsProcessed.incrementAndGet();
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
    keysProcessed.incrementAndGet();
    OmKeyInfo keyInfo = ozoneClient.getProxy().getKeyInfo(
        volumeName, bucketName, keyName, false);

    // Check if key should be processed based on replication config
    if (!shouldProcessKeyByReplicationType(keyInfo)) {
      return;
    }

    ObjectNode keyNode = JsonUtils.createObjectNode(null);
    keyNode.put("volumeName", volumeName);
    keyNode.put("bucketName", bucketName);
    keyNode.put("name", keyName);

    ArrayNode blocksArray = keyNode.putArray("blocks");
    boolean keyPass = true;
    Set<String> failedVerificationTypes = new HashSet<>();

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
            failedVerificationTypes.add(verifier.getType());
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
    if (keyPass) {
      keysPassed.incrementAndGet();
    } else {
      keysFailed.incrementAndGet();
      allKeysPassed.set(false);
      failedVerificationTypes.forEach(failedType -> failuresByType
          .computeIfAbsent(failedType, k -> new AtomicInteger(0))
          .incrementAndGet()
      );
    }

    if (!keyPass || allResults) {
      keysArray.add(keyNode);
    }
  }

  /**
   * Adds ShutdownHook to print summary statistics.
   */
  private void addShutdownHook() {
    ShutdownHookManager.get().addShutdownHook(() -> {
      if (endTime == 0) {
        endTime = System.nanoTime();
      }
      printSummary(System.err);
    }, DEFAULT_SHUTDOWN_HOOK_PRIORITY);
  }

  /**
   * Prints summary of replica verification run.
   *
   * @param out PrintStream
   */
  void printSummary(PrintStream out) {
    if (endTime == 0) {
      endTime = System.nanoTime();
    }

    long execTimeNanos = endTime - startTime;
    String execTime = DurationFormatUtils.formatDuration(TimeUnit.NANOSECONDS.toMillis(execTimeNanos), DURATION_FORMAT);

    long totalKeysProcessed = keysProcessed.get();
    long totalKeysPassed = keysPassed.get();
    long totalKeysFailed = keysFailed.get();

    out.println();
    out.println("***************************************************");
    out.println("REPLICA VERIFICATION SUMMARY");
    out.println("***************************************************");
    out.println("Status: " + (exception != null ? "Failed" :
        (totalKeysFailed == 0 ? "Success" : "Completed with failures")));
    out.println("Verification Scope: " + verificationScope);
    out.println("Verification Types: " + String.join(", ", verificationTypes));
    out.println("URI: " + uri);
    out.println();
    out.println("Number of Volumes processed: " + volumesProcessed.get());
    out.println("Number of Buckets processed: " + bucketsProcessed.get());
    out.println("Number of Keys processed: " + totalKeysProcessed);
    out.println();
    out.println("Keys passed verification: " + totalKeysPassed);
    out.println("Keys failed verification: " + totalKeysFailed);

    if (!failuresByType.isEmpty() && totalKeysFailed > 0) {
      out.println();
      for (String verificationType : verificationTypes) {
        long typeFailures = failuresByType.get(verificationType).get();
        if (typeFailures > 0) {
          out.println("Keys failed " + verificationType + " verification: " + typeFailures);
        }
      }
      out.println("Note: A key may fail multiple verification types, so total may exceed overall failures.");
    }

    out.println();
    out.println("Total Execution time: " + execTime);

    if (exception != null) {
      out.println();
      out.println("Exception: " + exception.getClass().getSimpleName() + ": " + exception.getMessage());
    }

    out.println("***************************************************");
  }

  /**
   * Check if the key should be processed based on replication config.
   * @param keyInfo the key to check
   * @return true if the key should be processed, false if it should be skipped
   */
  private boolean shouldProcessKeyByReplicationType(OmKeyInfo keyInfo) {
    Optional<ReplicationConfig> filterConfig = replication.fromParams(getConf());
    if (!filterConfig.isPresent()) {
      // No filter specified, include all keys
      return true;
    }

    ReplicationConfig keyReplicationConfig = keyInfo.getReplicationConfig();
    ReplicationConfig filter = filterConfig.get();

    // Process key only if both replication type and factor match
    return keyReplicationConfig.getReplicationType().equals(filter.getReplicationType())
        && keyReplicationConfig.getReplication().equals(filter.getReplication());
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
        description = "Check the container and replica states." +
            " Containers must be in [OPEN, CLOSING, QUASI_CLOSED, CLOSED] states," +
            " and it's replicas must be in [OPEN, CLOSING, QUASI_CLOSED, CLOSED] states" +
            " to pass the check. Any other states will fail the verification.",
        defaultValue = "false")
    private boolean doExecuteReplicaState;

  }
}
