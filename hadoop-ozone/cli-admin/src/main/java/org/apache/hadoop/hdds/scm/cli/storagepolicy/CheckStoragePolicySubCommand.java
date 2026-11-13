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

package org.apache.hadoop.hdds.scm.cli.storagepolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaInfo;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneKeyLocation;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.keys.KeyUri;
import picocli.CommandLine;

/**
 * Handler of the command that checks whether the replicas of a key are on the
 * storage type of the key's storage policy.
 *
 * <p>A replica matches when both its container storage type and the type of the volume it sits on
 * equal the storage type of the policy's creation tier. A replica on the fallback tier does not match,
 * and an unknown (null) type does not match.
 */
@CommandLine.Command(
    name = "check",
    description = "Check whether the replicas of a key are on the storage type of its storage policy",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class CheckStoragePolicySubCommand extends ScmSubcommand {

  @CommandLine.Mixin
  private KeyUri uri;

  @CommandLine.Option(
      names = {"--show-replicas"},
      description = "Show the storage types of every replica of every container of the key.")
  private boolean showReplicas;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    OzoneAddress address = uri.getValue();
    try (OzoneClient client = createOzoneClient(address)) {
      OzoneKeyDetails keyDetails = client.getObjectStore()
          .getVolume(address.getVolumeName())
          .getBucket(address.getBucketName())
          .getKey(address.getKeyName());
      System.out.println(buildReport(keyDetails, scmClient, showReplicas));
    }
  }

  protected OzoneClient createOzoneClient(OzoneAddress address) throws IOException {
    return address.createClient(getOzoneConf());
  }

  /**
   * Checks the key and builds the text of the report.
   *
   * @param withReplicas true to add the container and replica details
   */
  public String buildReport(OzoneKeyDetails keyDetails, ScmClient scmClient, boolean withReplicas)
      throws IOException {
    OzoneStoragePolicy policy = (OzoneStoragePolicy) keyDetails.getStoragePolicy();
    List<String> lines = new ArrayList<>();
    lines.add("Storage Policy for key '" + keyDetails.getVolumeName() + "/" + keyDetails.getBucketName() + "/"
        + keyDetails.getName() + "': " + (policy == null ? "UNKNOWN" : policy.name()));

    if (policy == null) {
      return String.join("\n", lines);
    }

    List<ContainerCheck> containerChecks = evaluate(keyDetails, scmClient);
    boolean keyMatches = keyMatches(containerChecks);
    lines.add("  Key Match Storage Policy: " + (!keyMatches ? "NO"
        : containerChecks.isEmpty() ? "YES (key has no data blocks)" : "YES"));

    if (withReplicas) {
      for (ContainerCheck containerCheck : containerChecks) {
        describeContainer(containerCheck, lines);
      }
    }
    return String.join("\n", lines);
  }

  /**
   * Decides, for every container of the key, whether its replicas are on the creation tier of the key's policy.
   * Each container is asked from SCM once, in the order of its first block. The key must have a policy.
   */
  List<ContainerCheck> evaluate(OzoneKeyDetails keyDetails, ScmClient scmClient) throws IOException {
    StorageType expected = keyDetails.getStoragePolicy().getCreationTier().getUniformStorageType();

    Set<Long> containerIds = new LinkedHashSet<>();
    for (OzoneKeyLocation location : keyDetails.getOzoneKeyLocations()) {
      containerIds.add(location.getContainerID());
    }

    List<ContainerCheck> containerChecks = new ArrayList<>();
    for (long containerId : containerIds) {
      List<ReplicaCheck> replicaChecks = new ArrayList<>();
      for (ContainerReplicaInfo replica : scmClient.getContainerReplicas(containerId)) {
        replicaChecks.add(new ReplicaCheck(replica, expected));
      }
      containerChecks.add(new ContainerCheck(containerId, replicaChecks));
    }
    return containerChecks;
  }

  /** A key matches when every container matches. A key without containers matches. */
  static boolean keyMatches(List<ContainerCheck> containerChecks) {
    for (ContainerCheck containerCheck : containerChecks) {
      if (!containerCheck.matches()) {
        return false;
      }
    }
    return true;
  }

  private static void describeContainer(ContainerCheck containerCheck, List<String> lines) {
    lines.add("Container ID: " + containerCheck.getContainerId());
    List<StorageType> storageTypes = new ArrayList<>();
    List<StorageType> volumeStorageTypes = new ArrayList<>();
    for (ReplicaCheck replicaCheck : containerCheck.getReplicaChecks()) {
      storageTypes.add(replicaCheck.getReplica().getStorageType());
      volumeStorageTypes.add(replicaCheck.getReplica().getVolumeStorageType());
    }
    lines.add("  Replica: " + describeStorageTypes(storageTypes)
        + ", Volumes: " + describeStorageTypes(volumeStorageTypes));
    lines.add("  Container Match Storage Policy: " + (containerCheck.matches() ? "YES" : "NO"));
    if (containerCheck.getReplicaChecks().isEmpty()) {
      lines.add("  No replicas reported by SCM");
    }
    for (ReplicaCheck replicaCheck : containerCheck.getReplicaChecks()) {
      ContainerReplicaInfo replica = replicaCheck.getReplica();
      DatanodeDetails datanode = replica.getDatanodeDetails();
      lines.add("  Datanode: " + datanode.getUuidString() + " (" + datanode.getHostName() + ", "
          + datanode.getNetworkLocation() + ")");
      lines.add("  Replica Storage Type: " + replica.getStorageType());
      lines.add("  Volume Storage Type: " + replica.getVolumeStorageType());
      lines.add("  Replica Match: " + (replicaCheck.matches() ? "YES"
          : "NO (" + String.join("; ", replicaCheck.getMismatchReasons()) + ")"));
    }
  }

  /**
   * Counts the types and prints them as {@code {[SSD x 2], [DISK x 1]}}, in the order of the StorageType enum.
   * A null type (SCM does not know it) prints as UNKNOWN, after the known types. No types print {@code {}}.
   */
  private static String describeStorageTypes(List<StorageType> types) {
    Map<StorageType, Integer> counts = new EnumMap<>(StorageType.class);
    int unknown = 0;
    for (StorageType type : types) {
      if (type == null) {
        unknown++;
      } else {
        counts.merge(type, 1, Integer::sum);
      }
    }
    List<String> groups = new ArrayList<>();
    counts.forEach((type, count) -> groups.add("[" + type + " x " + count + "]"));
    if (unknown > 0) {
      groups.add("[UNKNOWN x " + unknown + "]");
    }
    return "{" + String.join(", ", groups) + "}";
  }

  /**
   * Result of checking one container of the key. It matches when SCM reports at least one replica
   * and every replica matches.
   */
  static final class ContainerCheck {
    private final long containerId;
    private final List<ReplicaCheck> replicaChecks;

    ContainerCheck(long containerId, List<ReplicaCheck> replicaChecks) {
      this.containerId = containerId;
      this.replicaChecks = Collections.unmodifiableList(replicaChecks);
    }

    long getContainerId() {
      return containerId;
    }

    List<ReplicaCheck> getReplicaChecks() {
      return replicaChecks;
    }

    boolean matches() {
      if (replicaChecks.isEmpty()) {
        return false;
      }
      for (ReplicaCheck replicaCheck : replicaChecks) {
        if (!replicaCheck.matches()) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * Result of checking one replica of a container. It matches when both the container storage type and
   * the volume storage type equal the expected type. It keeps one reason for each type that differs.
   */
  static final class ReplicaCheck {
    private final ContainerReplicaInfo replica;
    private final List<String> mismatchReasons = new ArrayList<>();

    ReplicaCheck(ContainerReplicaInfo replica, StorageType expected) {
      this.replica = replica;
      addReasonIfDifferent("storageType", replica.getStorageType(), expected);
      addReasonIfDifferent("volumeStorageType", replica.getVolumeStorageType(), expected);
    }

    private void addReasonIfDifferent(String field, StorageType actual, StorageType expected) {
      if (actual != expected) {
        mismatchReasons.add(field + " " + actual + " != expected " + expected);
      }
    }

    ContainerReplicaInfo getReplica() {
      return replica;
    }

    boolean matches() {
      return mismatchReasons.isEmpty();
    }

    List<String> getMismatchReasons() {
      return Collections.unmodifiableList(mismatchReasons);
    }
  }
}
