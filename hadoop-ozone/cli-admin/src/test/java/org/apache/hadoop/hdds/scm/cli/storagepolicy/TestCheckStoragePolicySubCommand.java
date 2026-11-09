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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StoragePolicy;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.storagepolicy.CheckStoragePolicySubCommand.ContainerCheck;
import org.apache.hadoop.hdds.scm.cli.storagepolicy.CheckStoragePolicySubCommand.ReplicaCheck;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaInfo;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneKeyLocation;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import picocli.CommandLine;

/**
 * Tests for {@link CheckStoragePolicySubCommand}.
 *
 * <p>Rule tests call {@code evaluate} and assert on the result objects. Layout tests call
 * {@code buildReport} and assert on the text. SCM is a Mockito mock; no cluster is started.
 */
public class TestCheckStoragePolicySubCommand {

  private static final String VOLUME = "vol1";
  private static final String BUCKET = "bucket1";
  private static final String KEY = "key1";
  private static final String KEY_HEADER = "Storage Policy for key 'vol1/bucket1/key1':";
  // This module compiles with --release 8, so the Charset overloads of PrintStream and toString are unavailable.
  private static final String ENCODING = StandardCharsets.UTF_8.name();
  private static final ReplicationConfig RATIS_THREE =
      RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;

  private ScmClient scmClient;
  private CheckStoragePolicySubCommand cmd;

  @BeforeEach
  public void setup() throws UnsupportedEncodingException {
    scmClient = mock(ScmClient.class);
    cmd = new CheckStoragePolicySubCommand();
    System.setOut(new PrintStream(outContent, false, ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
  }

  /**
   * Each policy is checked against its own creation tier. When both storageType and volumeStorageType of all three
   * replicas equal that tier, the replicas, the container and the key all match.
   */
  @ParameterizedTest
  @CsvSource({"HOT,SSD", "WARM,DISK", "COLD,ARCHIVE"})
  public void testAllReplicasOnCreationTierMatch(OzoneStoragePolicy policy, StorageType expected)
      throws IOException {
    scmReturns(1L, replicas(1L, 3, expected, expected));

    List<ContainerCheck> checks = cmd.evaluate(keyDetails(policy, RATIS_THREE, 1L), scmClient);

    assertThat(CheckStoragePolicySubCommand.keyMatches(checks)).isTrue();
    assertThat(checks).hasSize(1);
    assertThat(checks.get(0).matches()).isTrue();
    assertThat(checks.get(0).getReplicaChecks()).hasSize(3).allMatch(ReplicaCheck::matches);
    assertThat(checks.get(0).getReplicaChecks())
        .allSatisfy(r -> assertThat(r.getMismatchReasons()).isEmpty());
  }

  /**
   * A HOT key expects SSD. A replica whose storage type or volumeStorageType is not a match returns NO.
   * The reasons name each type that differs, storageType first. Rows (storageType, volumeStorageType):
   * <ul>
   *   <li>SSD, DISK: the container type is right but the replica sits on a volume of another type.</li>
   *   <li>DISK, ARCHIVE: both types are wrong.</li>
   *   <li>DISK, DISK: strict rule. The fallback tier of HOT is DISK, but only the creation tier counts.</li>
   * </ul>
   */
  @ParameterizedTest
  @CsvSource({"SSD,DISK", "DISK,ARCHIVE", "DISK,DISK"})
  public void testReplicaOffCreationTierIsReported(StorageType storageType, StorageType volumeStorageType)
      throws IOException {
    // One reason for each type that is not SSD, storageType first.
    List<String> expectedReasons = new ArrayList<>();
    if (storageType != StorageType.SSD) {
      expectedReasons.add("storageType " + storageType + " != expected SSD");
    }
    if (volumeStorageType != StorageType.SSD) {
      expectedReasons.add("volumeStorageType " + volumeStorageType + " != expected SSD");
    }
    scmReturns(1L, replicas(1L, 3, storageType, volumeStorageType));

    List<ContainerCheck> checks = cmd.evaluate(keyDetails(OzoneStoragePolicy.HOT, RATIS_THREE, 1L), scmClient);

    assertThat(CheckStoragePolicySubCommand.keyMatches(checks)).isFalse();
    assertThat(checks.get(0).matches()).isFalse();
    assertThat(checks.get(0).getReplicaChecks()).hasSize(3).allSatisfy(replica -> {
      assertThat(replica.matches()).isFalse();
      assertThat(replica.getMismatchReasons()).containsExactlyElementsOf(expectedReasons);
    });
  }

  /**
   * One replica of three sits on the wrong volume although the container record says SSD (for example after a
   * volume was relabeled). That replica is NO, so the container is NO and the key is NO.
   * The other two replicas stay YES.
   */
  @Test
  public void testOneBadReplicaFailsContainerAndKey() throws IOException {
    scmReturns(1L,
        replica(1L, StorageType.SSD, StorageType.SSD),
        replica(1L, StorageType.SSD, StorageType.DISK),
        replica(1L, StorageType.SSD, StorageType.SSD));

    List<ContainerCheck> checks = cmd.evaluate(keyDetails(OzoneStoragePolicy.HOT, RATIS_THREE, 1L), scmClient);

    assertThat(CheckStoragePolicySubCommand.keyMatches(checks)).isFalse();
    assertThat(checks.get(0).matches()).isFalse();
    assertThat(checks.get(0).getReplicaChecks().stream().map(ReplicaCheck::matches).collect(Collectors.toList()))
        .containsExactly(true, false, true);
  }

  /** A key with two containers where only the second is bad. Results keep the block order; the key is NO. */
  @Test
  public void testOneBadContainerFailsKey() throws IOException {
    scmReturns(1L, replicas(1L, 3, StorageType.SSD, StorageType.SSD));
    scmReturns(2L, replicas(2L, 3, StorageType.SSD, StorageType.DISK));

    List<ContainerCheck> checks = cmd.evaluate(keyDetails(OzoneStoragePolicy.HOT, RATIS_THREE, 1L, 2L), scmClient);

    assertThat(checks.stream().map(ContainerCheck::getContainerId).collect(Collectors.toList()))
        .containsExactly(1L, 2L);
    assertThat(checks.get(0).matches()).isTrue();
    assertThat(checks.get(1).matches()).isFalse();
    assertThat(CheckStoragePolicySubCommand.keyMatches(checks)).isFalse();
  }

  /** Several blocks of one key can sit in the same container. SCM is asked about that container once. */
  @Test
  public void testSameContainerInSeveralBlocksIsQueriedOnce() throws IOException {
    scmReturns(7L, replicas(7L, 3, StorageType.SSD, StorageType.SSD));

    List<ContainerCheck> checks =
        cmd.evaluate(keyDetails(OzoneStoragePolicy.HOT, RATIS_THREE, 7L, 7L, 7L), scmClient);

    assertThat(checks).hasSize(1);
    verify(scmClient, times(1)).getContainerReplicas(7L);
  }

  /** SCM reports no replica for the container. Nothing proves the data is on the right tier, so it is NO. */
  @Test
  public void testContainerWithoutReplicasDoesNotMatch() throws IOException {
    scmReturns(1L);

    List<ContainerCheck> checks = cmd.evaluate(keyDetails(OzoneStoragePolicy.HOT, RATIS_THREE, 1L), scmClient);

    assertThat(checks.get(0).getReplicaChecks()).isEmpty();
    assertThat(checks.get(0).matches()).isFalse();
    assertThat(CheckStoragePolicySubCommand.keyMatches(checks)).isFalse();
  }

  /** A key with no data blocks has no container to check. It matches, and SCM is not called. */
  @Test
  public void testKeyWithoutBlocksMatchesAndNeverCallsScm() throws IOException {
    List<ContainerCheck> checks = cmd.evaluate(keyDetails(OzoneStoragePolicy.HOT, RATIS_THREE), scmClient);

    assertThat(checks).isEmpty();
    assertThat(CheckStoragePolicySubCommand.keyMatches(checks)).isTrue();
    verifyNoInteractions(scmClient);
  }

  /**
   * An EC(3,2) key has five replicas, one per replica index. All five on SSD match. When the replica with index 4
   * moves to a DISK volume, the key is NO, and index 4 is the only failed replica.
   */
  @Test
  public void testErasureCodedKeyChecksEveryReplicaIndex() throws IOException {
    ReplicationConfig ec = new ECReplicationConfig(3, 2);
    List<ContainerReplicaInfo> good = new ArrayList<>();
    for (int index = 1; index <= 5; index++) {
      good.add(replica(1L, StorageType.SSD, StorageType.SSD, index));
    }
    scmReturns(1L, good.toArray(new ContainerReplicaInfo[0]));
    assertThat(CheckStoragePolicySubCommand.keyMatches(cmd.evaluate(keyDetails(OzoneStoragePolicy.HOT, ec, 1L),
        scmClient))).isTrue();

    List<ContainerReplicaInfo> oneBad = new ArrayList<>(good);
    oneBad.set(3, replica(1L, StorageType.SSD, StorageType.DISK, 4));
    scmReturns(1L, oneBad.toArray(new ContainerReplicaInfo[0]));
    List<ContainerCheck> checks = cmd.evaluate(keyDetails(OzoneStoragePolicy.HOT, ec, 1L), scmClient);

    assertThat(CheckStoragePolicySubCommand.keyMatches(checks)).isFalse();
    List<ReplicaCheck> failed = checks.get(0).getReplicaChecks().stream()
        .filter(r -> !r.matches()).collect(Collectors.toList());
    assertThat(failed).hasSize(1);
    assertThat(failed.get(0).getReplica().getReplicaIndex()).isEqualTo(4);
  }

  /**
   * Without --show-replicas the report has the header, the policy line and the result, and no container lines.
   * The result is still computed, so a mismatch prints NO.
   */
  @Test
  public void testDefaultReportShowsOnlyPolicyAndResult() throws IOException {
    scmReturns(12L, replicas(12L, 3, StorageType.SSD, StorageType.DISK));

    String report = cmd.buildReport(keyDetails(OzoneStoragePolicy.HOT, RATIS_THREE, 12L), scmClient, false);

    // The result is computed even without --show-replicas: a mismatch must print NO.
    assertThat(report).isEqualTo(lines(
        KEY_HEADER,
        "  HOT{Creation Tier: [SSD x 3], Fallback Tier: [DISK x 3]}",
        "  Key Match Storage Policy: NO"));
    assertThat(report).doesNotContain("Container ID");
  }

  /** Without --show-replicas, a key whose replicas all match prints YES. */
  @Test
  public void testDefaultReportOfMatchingKeyIsYes() throws IOException {
    scmReturns(12L, replicas(12L, 3, StorageType.SSD, StorageType.SSD));

    String report = cmd.buildReport(keyDetails(OzoneStoragePolicy.HOT, RATIS_THREE, 12L), scmClient, false);

    assertThat(report).endsWith("  Key Match Storage Policy: YES");
  }

  /**
   * Pins the full text of --show-replicas for one container with three replicas: a wrong volume, a wrong
   * container type and a good replica. Each replica shows its datanode, both storage types and its own YES or NO.
   */
  @Test
  public void testDetailedReportShowsEveryReplica() throws IOException {
    DatanodeDetails dn1 = MockDatanodeDetails.createDatanodeDetails("dn1.example.com", "/rack1");
    DatanodeDetails dn2 = MockDatanodeDetails.createDatanodeDetails("dn2.example.com", "/rack2");
    DatanodeDetails dn3 = MockDatanodeDetails.createDatanodeDetails("dn3.example.com", "/rack3");
    scmReturns(12L,
        replica(12L, dn1, StorageType.SSD, StorageType.DISK, -1),
        replica(12L, dn2, StorageType.DISK, StorageType.SSD, -1),
        replica(12L, dn3, StorageType.SSD, StorageType.SSD, -1));

    String report = cmd.buildReport(keyDetails(OzoneStoragePolicy.HOT, RATIS_THREE, 12L), scmClient, true);

    assertThat(report).isEqualTo(lines(
        KEY_HEADER,
        "  HOT{Creation Tier: [SSD x 3], Fallback Tier: [DISK x 3]}",
        "  Key Match Storage Policy: NO",
        "Container ID: 12",
        "  Datanode: " + dn1.getUuidString() + " (dn1.example.com, /rack1)",
        "  Replica Storage Type: SSD",
        "  Volume Storage Type: DISK",
        "  Replica Match: NO (volumeStorageType DISK != expected SSD)",
        "  Datanode: " + dn2.getUuidString() + " (dn2.example.com, /rack2)",
        "  Replica Storage Type: DISK",
        "  Volume Storage Type: SSD",
        "  Replica Match: NO (storageType DISK != expected SSD)",
        "  Datanode: " + dn3.getUuidString() + " (dn3.example.com, /rack3)",
        "  Replica Storage Type: SSD",
        "  Volume Storage Type: SSD",
        "  Replica Match: YES",
        "  Container Match Storage Policy: NO"));
  }

  /**
   * Runs the command the way the shell does: picocli parses the key URI and --show-replicas, then execute reads the
   * key, asks SCM and prints the report. Three replicas on SSD match, so every line says YES. Only the Ozone client
   * and the SCM client are mocks.
   */
  @Test
  public void testPicocliShowReplicasPrintsMatchingKeyReport() throws IOException, UnsupportedEncodingException {
    DatanodeDetails dn1 = MockDatanodeDetails.createDatanodeDetails("dn1.example.com", "/rack1");
    DatanodeDetails dn2 = MockDatanodeDetails.createDatanodeDetails("dn2.example.com", "/rack2");
    DatanodeDetails dn3 = MockDatanodeDetails.createDatanodeDetails("dn3.example.com", "/rack3");

    scmReturns(12L,
        replica(12L, dn1, StorageType.SSD, StorageType.SSD, -1),
        replica(12L, dn2, StorageType.SSD, StorageType.SSD, -1),
        replica(12L, dn3, StorageType.SSD, StorageType.SSD, -1));

    OzoneClient ozoneClient = ozoneClientReturning(keyDetails(OzoneStoragePolicy.HOT, RATIS_THREE, 12L));
    CheckStoragePolicySubCommand command = new CheckStoragePolicySubCommand() {
      @Override
      protected OzoneClient createOzoneClient(OzoneAddress address) {
        return ozoneClient;
      }
    };

    new CommandLine(command).parseArgs(VOLUME + "/" + BUCKET + "/" + KEY, "--show-replicas");
    command.execute(scmClient);

    assertThat(outContent.toString(ENCODING)).isEqualToNormalizingNewlines(lines(
        KEY_HEADER,
        "  HOT{Creation Tier: [SSD x 3], Fallback Tier: [DISK x 3]}",
        "  Key Match Storage Policy: YES",
        "Container ID: 12",
        "  Datanode: " + dn1.getUuidString() + " (dn1.example.com, /rack1)",
        "  Replica Storage Type: SSD",
        "  Volume Storage Type: SSD",
        "  Replica Match: YES",
        "  Datanode: " + dn2.getUuidString() + " (dn2.example.com, /rack2)",
        "  Replica Storage Type: SSD",
        "  Volume Storage Type: SSD",
        "  Replica Match: YES",
        "  Datanode: " + dn3.getUuidString() + " (dn3.example.com, /rack3)",
        "  Replica Storage Type: SSD",
        "  Volume Storage Type: SSD",
        "  Replica Match: YES",
        "  Container Match Storage Policy: YES", ""));
  }

  /** When both types are wrong, the detailed report joins the two reasons on one line with "; ". */
  @Test
  public void testDetailedReportListsBothReasonsOnOneLine() throws IOException {
    scmReturns(12L, replicas(12L, 1, StorageType.DISK, StorageType.ARCHIVE));

    String report = cmd.buildReport(keyDetails(OzoneStoragePolicy.HOT, RATIS_THREE, 12L), scmClient, true);

    assertThat(report).contains("  Replica Match: NO (storageType DISK != expected SSD; "
        + "volumeStorageType ARCHIVE != expected SSD)");
  }

  /** A container with no replicas prints a "No replicas reported by SCM" line, and the container and key are NO. */
  @Test
  public void testDetailedReportOfContainerWithoutReplicas() throws IOException {
    scmReturns(12L);

    String report = cmd.buildReport(keyDetails(OzoneStoragePolicy.HOT, RATIS_THREE, 12L), scmClient, true);

    assertThat(report).endsWith(lines(
        "Container ID: 12",
        "  No replicas reported by SCM",
        "  Container Match Storage Policy: NO"));
    assertThat(report).contains("  Key Match Storage Policy: NO");
  }

  /** Every container gets its own block in block order, with its own container result (YES for 1, NO for 2). */
  @Test
  public void testDetailedReportOfSecondContainer() throws IOException {
    scmReturns(1L, replicas(1L, 3, StorageType.SSD, StorageType.SSD));
    scmReturns(2L, replicas(2L, 3, StorageType.SSD, StorageType.DISK));

    String report = cmd.buildReport(keyDetails(OzoneStoragePolicy.HOT, RATIS_THREE, 1L, 2L), scmClient, true);

    assertThat(report).contains("Container ID: 1", "Container ID: 2");
    assertThat(report.indexOf("Container ID: 1")).isLessThan(report.indexOf("Container ID: 2"));
    assertThat(report).contains("  Container Match Storage Policy: YES", "  Container Match Storage Policy: NO");
  }

  /**
   * The policy line shows the creation tier and the fallback tier. HOT falls back to DISK; WARM and COLD have no
   * fallback and print "[disable]".
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "HOT|SSD|[DISK x 3]",
      "WARM|DISK|[disable]",
      "COLD|ARCHIVE|[disable]"})
  public void testSummaryLineShowsCreationAndFallbackTier(OzoneStoragePolicy policy, StorageType creation,
      String fallback) throws IOException {
    scmReturns(1L, replicas(1L, 3, creation, creation));

    String report = cmd.buildReport(keyDetails(policy, RATIS_THREE, 1L), scmClient, false);

    assertThat(report.split("\n")[1]).isEqualTo(
        "  " + policy.name() + "{Creation Tier: [" + creation + " x 3], Fallback Tier: " + fallback + "}");
  }

  /** The tier count on the policy line is the number of nodes the key needs: an EC(3,2) key shows "x 5". */
  @Test
  public void testSummaryLineUsesRequiredNodesOfReplicationConfig() throws IOException {
    ReplicationConfig ec = new ECReplicationConfig(3, 2);
    scmReturns(1L, replicas(1L, 5, StorageType.SSD, StorageType.SSD));

    String report = cmd.buildReport(keyDetails(OzoneStoragePolicy.HOT, ec, 1L), scmClient, false);

    assertThat(report.split("\n")[1])
        .isEqualTo("  HOT{Creation Tier: [SSD x 5], Fallback Tier: [DISK x 5]}");
  }

  /** A key with no storage policy prints a notice only. There is nothing to compare, so SCM is not called. */
  @Test
  public void testKeyWithoutStoragePolicyPrintsNoticeAndSkipsScm() throws IOException {
    String report = cmd.buildReport(keyDetails(null, RATIS_THREE, 1L), scmClient, true);

    assertThat(report).isEqualTo(lines(KEY_HEADER, "  Storage Policy: none recorded for this key"));
    verifyNoInteractions(scmClient);
  }

  /** A key with a policy but no data blocks prints YES with a note that explains why, and SCM is not called. */
  @Test
  public void testKeyWithoutBlocksReportsYesWithNote() throws IOException {
    String report = cmd.buildReport(keyDetails(OzoneStoragePolicy.WARM, RATIS_THREE), scmClient, true);

    assertThat(report).isEqualTo(lines(
        KEY_HEADER,
        "  WARM{Creation Tier: [DISK x 3], Fallback Tier: [disable]}",
        "  Key Match Storage Policy: YES (key has no data blocks)"));
    verifyNoInteractions(scmClient);
  }

  /** The "check" command is a subcommand of "ozone admin storagepolicy". */
  @Test
  public void testCheckIsRegisteredUnderStoragePolicyCommands() {
    CommandLine storagePolicy = new CommandLine(new StoragePolicyCommands());

    assertThat(storagePolicy.getSubcommands()).containsKey("check");
  }

  /** Builds an Ozone client mock whose volume, bucket and key lookups for vol1/bucket1/key1 return this key. */
  private static OzoneClient ozoneClientReturning(OzoneKeyDetails keyDetails) throws IOException {
    OzoneClient ozoneClient = mock(OzoneClient.class);
    ObjectStore objectStore = mock(ObjectStore.class);
    OzoneVolume volume = mock(OzoneVolume.class);
    OzoneBucket bucket = mock(OzoneBucket.class);
    when(ozoneClient.getObjectStore()).thenReturn(objectStore);
    when(objectStore.getVolume(VOLUME)).thenReturn(volume);
    when(volume.getBucket(BUCKET)).thenReturn(bucket);
    when(bucket.getKey(KEY)).thenReturn(keyDetails);
    return ozoneClient;
  }

  /** Makes the SCM mock return these replicas for the container. No replica means an empty list, as in SCM. */
  private void scmReturns(long containerId, ContainerReplicaInfo... replicas) throws IOException {
    when(scmClient.getContainerReplicas(containerId)).thenReturn(Arrays.asList(replicas));
  }

  /** Builds {@code count} replicas of one container, all with the same storage type and volume storage type. */
  private static ContainerReplicaInfo[] replicas(long containerId, int count, StorageType storageType,
      StorageType volumeStorageType) {
    ContainerReplicaInfo[] replicas = new ContainerReplicaInfo[count];
    for (int i = 0; i < count; i++) {
      replicas[i] = replica(containerId, storageType, volumeStorageType);
    }
    return replicas;
  }

  /** Builds one Ratis replica (no replica index) on a random datanode. */
  private static ContainerReplicaInfo replica(long containerId, StorageType storageType,
      StorageType volumeStorageType) {
    return replica(containerId, storageType, volumeStorageType, -1);
  }

  /** Builds one replica with a replica index (used for EC) on a random datanode. */
  private static ContainerReplicaInfo replica(long containerId, StorageType storageType,
      StorageType volumeStorageType, int replicaIndex) {
    return replica(containerId, MockDatanodeDetails.randomDatanodeDetails(), storageType, volumeStorageType,
        replicaIndex);
  }

  /** Builds one CLOSED replica on the given datanode. The other overloads call this one. */
  private static ContainerReplicaInfo replica(long containerId, DatanodeDetails datanode, StorageType storageType,
      StorageType volumeStorageType, int replicaIndex) {
    return new ContainerReplicaInfo.Builder()
        .setContainerID(containerId)
        .setState("CLOSED")
        .setDatanodeDetails(datanode)
        .setReplicaIndex(replicaIndex)
        .setStorageType(storageType)
        .setVolumeStorageType(volumeStorageType)
        .build();
  }

  /**
   * Builds a key with one 10-byte block per given container ID, in the given order. A repeated ID gives several
   * blocks in the same container. No ID gives a key without blocks. A null policy gives a key without a policy.
   */
  private static OzoneKeyDetails keyDetails(StoragePolicy policy, ReplicationConfig replicationConfig,
      long... containerIds) {
    List<OzoneKeyLocation> locations = new ArrayList<>();
    long offset = 0;
    for (long containerId : containerIds) {
      locations.add(new OzoneKeyLocation(containerId, 1000 + locations.size(), 10, 0, offset));
      offset += 10;
    }
    return new OzoneKeyDetails(VOLUME, BUCKET, KEY, offset, 0L, 0L, locations, replicationConfig,
        Collections.emptyMap(), null, null, true, "owner", Collections.emptyMap(), null, policy);
  }

  private static String lines(String... lines) {
    return String.join("\n", lines);
  }
}
