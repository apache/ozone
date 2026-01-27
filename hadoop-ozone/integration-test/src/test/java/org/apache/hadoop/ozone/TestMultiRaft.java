package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.multiraft.OMHAMultiRaftMetrics;
import org.apache.hadoop.ozone.om.ratis.BucketStateMachine;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.StateMachine;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertFalse;


/**
 * Test MultiRaft.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class TestMultiRaft {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestMultiRaft.class);
  private static final String VOLUME_NAME = "testvolume";
  private static final String BUCKET_NAME = "testbucket";

  private MiniOzoneHAClusterImpl cluster = null;
  private OzoneConfiguration conf;

  /**
   * Create a MiniOzoneHAClusterImpl for testing.
   *
   * @throws IOException
   */
  private MiniOzoneHAClusterImpl initClusterWithMultiRaft(Boolean isMultiRaftEnabled)
      throws IOException, InterruptedException, TimeoutException {
    return initClusterWithMultiRaft(isMultiRaftEnabled, null);
  }

  private MiniOzoneHAClusterImpl initClusterWithMultiRaft(Boolean isMultiRaftEnabled, Integer maxMultiRaftGroup)
      throws IOException, TimeoutException, InterruptedException {
    conf = new OzoneConfiguration();
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    String omServiceId = "omServiceId1";
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_ADMINISTRATORS,
        OZONE_ADMINISTRATORS_WILDCARD);
    conf.setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, isMultiRaftEnabled);
    if (maxMultiRaftGroup != null) {
      conf.setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, maxMultiRaftGroup);
    }

    int numOfOMs = 3;
    MiniOzoneHAClusterImpl currentCluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setSCMServiceId(scmId)
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(numOfOMs)
        .build();
    currentCluster.waitForClusterToBeReady();
    return currentCluster;
  }

  /**
   * Shutdown MiniOzoneHAClusterImpl.
   */
  @AfterEach
  void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  void testRaftGroupsAndStateMachinesWhenMultiRaftDisables()
      throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(false);
    OzoneManager om = cluster.getOMLeader();
    assertEquals(1, om.getOmRaftGroups().size());
    assertEquals(1, om.getStateMachines().size());
  }

  @Test
  void testDefaultRaftGroupsAndStateMachinesWhenMultiRaftEnabled()
      throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true);
    OzoneManager om = cluster.getOMLeader();

    waitFor(
        () -> om.getOmRaftGroups().size() == 5,
        100,
        80000
    );
    assertEquals(5, om.getOmRaftGroups().size());
    waitFor(
        () -> om.getStateMachines().size() == 5,
        100,
        80000
    );
    assertEquals(5, om.getStateMachines().size());
  }

  @Test
  void testRaftGroupsAndStateMachinesWhenMultiRaftEnabled()
      throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 10);
    OzoneManager om = cluster.getOMLeader();
    int expectedRaftGroupsCount = 11;
    waitFor(
        () -> om.getOmRaftGroups().size() == expectedRaftGroupsCount
              && om.getStateMachines().size() == expectedRaftGroupsCount,
        100,
        80000
    );
    assertEquals(expectedRaftGroupsCount, om.getOmRaftGroups().size());
    assertEquals(expectedRaftGroupsCount, om.getStateMachines().size());
  }

  @Test
  void testChangeMultiRaftConfig() throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);
    int expectedRaftGroupsCount;

    ClientProtocol ozoneClient = cluster.createClient().getProxy();
    ozoneClient.createVolume(VOLUME_NAME);
    ozoneClient.createBucket(VOLUME_NAME, BUCKET_NAME);
    String key = "testkey";
    writeKey(ozoneClient, VOLUME_NAME, BUCKET_NAME, key);

    cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
    cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
    cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    OzoneManager om1 = cluster.getOzoneManager(0);
    OzoneManager om2 = cluster.getOzoneManager(1);
    OzoneManager om3 = cluster.getOzoneManager(2);

    expectedRaftGroupsCount = 1;
    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, expectedRaftGroupsCount);

    String key1 = "testkey1";
    writeKey(ozoneClient, VOLUME_NAME, BUCKET_NAME, key1);

    cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
    cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
    cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();
    expectedRaftGroupsCount = 5;
    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, expectedRaftGroupsCount);
    assertEquals(5, om1.getOmRaftGroups().size());
    assertEquals(5, om2.getOmRaftGroups().size());
    assertEquals(5, om3.getOmRaftGroups().size());

    String key2 = "testkey2";
    writeKey(ozoneClient, VOLUME_NAME, BUCKET_NAME, key2);

    cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
    cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
    cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    expectedRaftGroupsCount = 1;
    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, expectedRaftGroupsCount);

    String key3 = "testkey3";
    writeKey(ozoneClient, VOLUME_NAME, BUCKET_NAME, key3);

    checkKeyReading(VOLUME_NAME, BUCKET_NAME, key);
    checkKeyReading(VOLUME_NAME, BUCKET_NAME, key1);
    checkKeyReading(VOLUME_NAME, BUCKET_NAME, key2);
    checkKeyReading(VOLUME_NAME, BUCKET_NAME, key3);
  }

  @Test
  void testChangeMultiRaftGroupsSize() throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);
    int expectedRaftGroupsCount;
    cluster.getOzoneManager(0).getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 10);
    cluster.getOzoneManager(1).getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 10);
    cluster.getOzoneManager(2).getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 10);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    OzoneManager om1 = cluster.getOzoneManager(0);
    OzoneManager om2 = cluster.getOzoneManager(1);
    OzoneManager om3 = cluster.getOzoneManager(2);
    expectedRaftGroupsCount = 11;
    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, expectedRaftGroupsCount);

    ClientProtocol ozoneClient = cluster.createClient().getProxy();
    ozoneClient.createVolume(VOLUME_NAME);
    ozoneClient.createBucket(VOLUME_NAME, BUCKET_NAME);
    OzoneBucket bucketDetails = ozoneClient.getBucketDetails(VOLUME_NAME, BUCKET_NAME);
    assertNotNull(bucketDetails);
    assertEquals(BUCKET_NAME, bucketDetails.getName());
    assertEquals(VOLUME_NAME, bucketDetails.getVolumeName());

    String key = "testkey";
    writeKey(ozoneClient, VOLUME_NAME, BUCKET_NAME, key);

    OzoneKeyDetails keyDetails = ozoneClient.getKeyDetails(VOLUME_NAME, BUCKET_NAME, key);
    assertNotNull(keyDetails);
    assertEquals("testkey", keyDetails.getName());

    checkKeyReading(VOLUME_NAME, BUCKET_NAME, key);
  }

  @Test
  void testWriteRequestsExecutesThroughBucketStateMachines()
      throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 1);

    OzoneManager om1 = cluster.getOzoneManager(0);
    OzoneManager om2 = cluster.getOzoneManager(1);
    OzoneManager om3 = cluster.getOzoneManager(2);

    int expectedRaftGroupsCount = 2;
    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, expectedRaftGroupsCount);

    ClientProtocol ozoneClient = cluster.createClient().getProxy();
    ozoneClient.createVolume(VOLUME_NAME);
    String bucket1 = "111a";
    ozoneClient.createBucket(VOLUME_NAME, bucket1);
    String bucket2 = "222a";
    ozoneClient.createBucket(VOLUME_NAME, bucket2);
    String bucket3 = "333a";
    ozoneClient.createBucket(VOLUME_NAME, bucket3);
    String bucket4 = "444a";
    ozoneClient.createBucket(VOLUME_NAME, bucket4);

    String key = "testkey";

    writeKey(ozoneClient, VOLUME_NAME, bucket1, key);
    waitTermIndex(om1, 4L);
    checkLastAppliedIndex(4L, om1);

    writeKey(ozoneClient, VOLUME_NAME, bucket2, key);
    waitTermIndex(om1, 8L);
    checkLastAppliedIndex(8L, om1);

    writeKey(ozoneClient, VOLUME_NAME, bucket3, key);
    waitTermIndex(om1, 12L);
    checkLastAppliedIndex(12L, om1);

    writeKey(ozoneClient, VOLUME_NAME, bucket4, key);
    waitTermIndex(om1, 16L);
    checkLastAppliedIndex(16L, om1);

    checkKeyReading(VOLUME_NAME, bucket1, key);
    checkKeyReading(VOLUME_NAME, bucket2, key);
    checkKeyReading(VOLUME_NAME, bucket3, key);
    checkKeyReading(VOLUME_NAME, bucket4, key);
  }

  @Test
  void testKeyConsistencyAfterMultiRaftReconfiguration() throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);
    int expectedRaftGroupsCount;

    ClientProtocol ozoneClient = cluster.createClient().getProxy();
    ozoneClient.createVolume(VOLUME_NAME);
    ozoneClient.createBucket(VOLUME_NAME, BUCKET_NAME);

    cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
    cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
    cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    OzoneManager om1 = cluster.getOzoneManager(0);
    OzoneManager om2 = cluster.getOzoneManager(1);
    OzoneManager om3 = cluster.getOzoneManager(2);

    expectedRaftGroupsCount = 1;
    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, expectedRaftGroupsCount);

    String key1 = "testkey1";
    writeKey(ozoneClient, VOLUME_NAME, BUCKET_NAME, key1);
    long keyUpdateId1 = getKeyUpdateId(VOLUME_NAME, BUCKET_NAME, key1);
    cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
    cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
    cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    expectedRaftGroupsCount = 5;
    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, expectedRaftGroupsCount);
    assertEquals(5, om2.getOmRaftGroups().size());

    writeKey(ozoneClient, VOLUME_NAME, BUCKET_NAME, key1, "updated text 1");
    // verify updated value is visible after enabling MultiRaft
    waitFor(
        () -> isKeyEquals(VOLUME_NAME, BUCKET_NAME, key1, "updated text 1"),
        500,
        2000
    );
    checkKeyReading(VOLUME_NAME, BUCKET_NAME, key1, "updated text 1");

    long keyUpdateId2 = getKeyUpdateId(VOLUME_NAME, BUCKET_NAME, key1);
    assertTrue(keyUpdateId2 < keyUpdateId1);
    cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
    cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
    cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    expectedRaftGroupsCount = 1;
    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, expectedRaftGroupsCount);

    writeKey(ozoneClient, VOLUME_NAME, BUCKET_NAME, key1, "updated text 2");
    // verify updated value is visible after disabling MultiRaft
    waitFor(
            () -> isKeyEquals(VOLUME_NAME, BUCKET_NAME, key1, "updated text 2"),
            500,
            2000
    );
    checkKeyReading(VOLUME_NAME, BUCKET_NAME, key1, "updated text 2");
    long keyUpdateId3 = getKeyUpdateId(VOLUME_NAME, BUCKET_NAME, key1);
    assertTrue(keyUpdateId3 > keyUpdateId2);
  }

  @Test
  void testCleaningRatisDirectory() throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);
    int expectedRaftGroupsCount = 5;
    OzoneManager om1 = cluster.getOzoneManager(0);
    OzoneManager om2 = cluster.getOzoneManager(1);
    OzoneManager om3 = cluster.getOzoneManager(2);

    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, expectedRaftGroupsCount);

    UUID mainGroupUuid = cluster.getOzoneManager(0).getOmRatisServer().getCurrentRaftGroupId().getUuid();
    List<String> dirsListBefore0 = getDirsList(om1.getConfiguration());
    assertTrue(dirsListBefore0.contains(mainGroupUuid.toString()));
    List<String> dirsListBefore1 = getDirsList(om2.getConfiguration());
    assertTrue(dirsListBefore1.contains(mainGroupUuid.toString()));
    List<String> dirsListBefore2 = getDirsList(om3.getConfiguration());
    assertTrue(dirsListBefore2.contains(mainGroupUuid.toString()));

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, expectedRaftGroupsCount);
    waitMultiRaftTerm(2);
    checkRemovedDirs(om1, mainGroupUuid, dirsListBefore0);
    checkRemovedDirs(om2, mainGroupUuid, dirsListBefore1);
    checkRemovedDirs(om3, mainGroupUuid, dirsListBefore2);
  }

  @Test
  void testGroupsCorrectCreatingWhenLeaderChangingBetweenReconcilerCycles()
      throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);
    int expectedRaftGroupsCount = 5;

    OzoneManager om1 = cluster.getOzoneManager(0);
    OzoneManager om2 = cluster.getOzoneManager(1);
    OzoneManager om3 = cluster.getOzoneManager(2);

    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, expectedRaftGroupsCount);

    OzoneManager omLeader = cluster.getOMLeader();

    cluster.shutdownOzoneManager(omLeader);
    cluster.restartOzoneManager(omLeader, false);

    waitOneOfOmRaftGroupsSizeOnNodesLess(om1, om2, om3, expectedRaftGroupsCount);

    waitLeaderElection();

    Stream.of(om1, om2, om3)
        .min(Comparator.comparingInt(el -> el.getOmRaftGroups().size()))
        .ifPresent(it -> {
          try {
            cluster.getOMLeader().transferLeadership(it.getOMNodeId());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });

    waitLeaderElection();

    OzoneManager newOmLeader = cluster.getOMLeader();
    cluster.shutdownOzoneManager(newOmLeader);
    cluster.restartOzoneManager(newOmLeader, false);

    waitLeaderElection();

    waitMultiRaftTerm(2);
    waitOmRaftGroupsSizeOnNodesEqual(cluster.getOzoneManager(0), cluster.getOzoneManager(1), cluster.getOzoneManager(2),
        expectedRaftGroupsCount);
    assertAll("Assert groups count",
        () -> assertEquals(5, cluster.getOzoneManager(0).getOmRaftGroups().size()),
        () -> assertEquals(5, cluster.getOzoneManager(1).getOmRaftGroups().size()),
        () -> assertEquals(5, cluster.getOzoneManager(2).getOmRaftGroups().size())
    );
  }

  @Test
  void testMultiRaftTermIncreasing()
      throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);
    int expectedRaftGroupsCount;

    OzoneManager om1 = cluster.getOzoneManager(0);
    OzoneManager om2 = cluster.getOzoneManager(1);
    OzoneManager om3 = cluster.getOzoneManager(2);

    expectedRaftGroupsCount = 5;
    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, expectedRaftGroupsCount);

    OzoneManager omLeader = cluster.getOMLeader();

    waitMultiRaftTerm(1);

    cluster.shutdownOzoneManager(omLeader);
    cluster.restartOzoneManager(omLeader, false);

    waitMultiRaftTerm(1);
    assertEquals(1, cluster.getOMLeader().getCurrentMultiRaftTerm());

    waitLeaderElection();

    OzoneManager newOmLeader = cluster.getOMLeader();
    cluster.shutdownOzoneManager(newOmLeader);
    cluster.restartOzoneManager(newOmLeader, false);

    waitLeaderElection();

    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, expectedRaftGroupsCount);
    waitMultiRaftTerm(2);
    assertEquals(2, cluster.getOMLeader().getCurrentMultiRaftTerm());
  }

  @Test
  void testSetNotNumberConfigParam()
      throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);

    OzoneManager om1 = cluster.getOzoneManager(0);

    om1.getConfiguration().set(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, "abcd");

    cluster.shutdownOzoneManager(om1);
    NumberFormatException numberFormatException =
        assertThrows(NumberFormatException.class, () -> cluster.restartOzoneManager(om1, false));
    assertEquals("For input string: \"abcd\"", numberFormatException.getMessage());
  }

  @Test
  void testSetNegativeNumberConfigParam()
      throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);

    OzoneManager om1 = cluster.getOzoneManager(0);
    OzoneManager om2 = cluster.getOzoneManager(1);
    OzoneManager om3 = cluster.getOzoneManager(2);

    om1.getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, -15);
    om2.getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, -15);
    om3.getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, -15);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();
    // Value -15 specified in config is invalid -> fall back to the default
    int expectedRaftGroupsCount = 7;
    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, expectedRaftGroupsCount);

    assertEquals(7, om1.getOmRaftGroups().size());
    assertEquals(7, om2.getOmRaftGroups().size());
    assertEquals(7, om3.getOmRaftGroups().size());
  }

  @Test
  void testSetZeroConfigParam() throws Exception {
    cluster = initClusterWithMultiRaft(true);

    OzoneManager om1 = cluster.getOzoneManager(0);
    OzoneManager om2 = cluster.getOzoneManager(1);
    OzoneManager om3 = cluster.getOzoneManager(2);

    om1.getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 0);
    om2.getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 0);
    om3.getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 0);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();
    // Value 0 specified in config is invalid -> fall back to the default
    int expectedRaftGroupsCount = 7;
    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, expectedRaftGroupsCount);

    assertEquals(7, om1.getOmRaftGroups().size());
    assertEquals(7, om2.getOmRaftGroups().size());
    assertEquals(7, om3.getOmRaftGroups().size());
  }

  @Test
  void testMetricAfterGroupCreating()
      throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);

    OzoneManager om1 = cluster.getOzoneManager(0);
    OzoneManager om2 = cluster.getOzoneManager(1);
    OzoneManager om3 = cluster.getOzoneManager(2);

    int expectedRaftGroupsCount = 5;
    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, expectedRaftGroupsCount);

    checkNodeStatistic(om1, 5);
    checkNodeStatistic(om2, 5);
    checkNodeStatistic(om3, 5);
  }

  @Test
  void testMetricAfterGroupReconfiguration()
      throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);

    OzoneManager om1 = cluster.getOzoneManager(0);
    OzoneManager om2 = cluster.getOzoneManager(1);
    OzoneManager om3 = cluster.getOzoneManager(2);

    cluster.getOzoneManager(0).getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 10);
    cluster.getOzoneManager(1).getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 10);
    cluster.getOzoneManager(2).getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 10);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    int expectedRaftGroupsCount = 11;
    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, expectedRaftGroupsCount);

    checkNodeStatistic(om1, 11);
    checkNodeStatistic(om2, 11);
    checkNodeStatistic(om3, 11);
  }

  @Test
  void testMetricAfterDisableMultiRaft()
      throws InterruptedException, TimeoutException, IOException {
    cluster = initClusterWithMultiRaft(true, 4);

    OzoneManager om1 = cluster.getOzoneManager(0);
    OzoneManager om2 = cluster.getOzoneManager(1);
    OzoneManager om3 = cluster.getOzoneManager(2);

    ClientProtocol ozoneClient = cluster.createClient().getProxy();
    ozoneClient.createVolume(VOLUME_NAME);
    ozoneClient.createBucket(VOLUME_NAME, BUCKET_NAME);

    List<String> keys = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      String key = "key" + i;
      writeKey(ozoneClient, VOLUME_NAME, BUCKET_NAME, key);
      keys.add(key);
    }

    cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
    cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);
    cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, false);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();

    int expectedRaftGroupsCount = 1;
    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, expectedRaftGroupsCount);

    checkNodeStatistic(om1, 1);
    checkNodeStatistic(om2, 1);
    checkNodeStatistic(om3, 1);

    for (String k : keys) {
      checkKeyReading(VOLUME_NAME, BUCKET_NAME, k);
    }
  }

  @Test
  void testKeyReadingAfterEnableMultiRaft() throws Exception {
    cluster = initClusterWithMultiRaft(false);

    ClientProtocol ozoneClient = cluster.createClient().getProxy();
    ozoneClient.createVolume(VOLUME_NAME);
    ozoneClient.createBucket(VOLUME_NAME, BUCKET_NAME);

    writeKey(ozoneClient, VOLUME_NAME, BUCKET_NAME, "key1");
    checkKeyReading(VOLUME_NAME, BUCKET_NAME, "key1");

    cluster.getOzoneManager(0).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
    cluster.getOzoneManager(1).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
    cluster.getOzoneManager(2).getConfiguration().setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
    cluster.getOzoneManager(0).getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 4);
    cluster.getOzoneManager(1).getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 4);
    cluster.getOzoneManager(2).getConfiguration().setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 4);

    cluster.restartOzoneManager();
    cluster.waitForClusterToBeReady();
    waitLeaderElection();

    OzoneManager om1 = cluster.getOzoneManager(0);
    OzoneManager om2 = cluster.getOzoneManager(1);
    OzoneManager om3 = cluster.getOzoneManager(2);

    int expectedRaftGroupsCount = 5;
    waitOmRaftGroupsSizeOnNodesEqual(om1, om2, om3, expectedRaftGroupsCount);

    checkKeyReading(VOLUME_NAME, BUCKET_NAME, "key1");
    writeKey(ozoneClient, VOLUME_NAME, BUCKET_NAME, "key2");
    checkKeyReading(VOLUME_NAME, BUCKET_NAME, "key2");
  }

  @Test
  void testRatisDirectoryExistsForEveryRaftGroup() throws Exception {
    cluster = initClusterWithMultiRaft(true, 4);

    OzoneManager om0 = cluster.getOzoneManager(0);
    OzoneManager om1 = cluster.getOzoneManager(1);
    OzoneManager om2 = cluster.getOzoneManager(2);

    int expectedRaftGroupsCount = 5;
    waitOmRaftGroupsSizeOnNodesEqual(om0, om1, om2, expectedRaftGroupsCount);

    for (OzoneManager om : Arrays.asList(om0, om1, om2)) {
      List<String> dirNames = getDirsList(om.getConfiguration());
      assertFalse(dirNames.isEmpty(), "Ratis metadata dir is empty on " + om.getOMNodeId());

      for (RaftGroupId gid : om.getOmRaftGroups().keySet()) {
        String uuid = gid.getUuid().toString();
        boolean present = dirNames.stream().anyMatch(n -> n.equals(uuid) || n.contains(uuid));
        assertTrue(present, "No ratis dir for group " + uuid + " on OM " + om.getOMNodeId() + ". dirs=" + dirNames);
      }
    }
  }

  private static void checkNodeStatistic(OzoneManager om, int omRaftGroups)
      throws TimeoutException, InterruptedException {
    OMHAMultiRaftMetrics omMultiRaftMetrics = om.getOmMultiRaftMetrics();
    omMultiRaftMetrics.getOmRaftGroupsCount();
    waitFor(() -> omMultiRaftMetrics.getOmRaftGroupsCount() == omRaftGroups, 100, 100_000);
    Assertions.assertEquals(omRaftGroups, omMultiRaftMetrics.getOmRaftGroupsCount());

    MetricsCollectorImpl omMultiRaftMetricsCollector = new MetricsCollectorImpl();
    omMultiRaftMetrics.getMetrics(omMultiRaftMetricsCollector, true);
    Assertions.assertEquals(3, omMultiRaftMetricsCollector.getRecords().size());

    Map<RaftGroupId, String> omHaInfoRaftGroupsLeaders = om.getOmhaMetrics().getOmHaInfoRaftGroupsLeaders();
    waitFor(() -> om.getOmhaMetrics().getOmHaInfoRaftGroupsLeaders().size() == omRaftGroups, 100, 100_000);
    MetricsCollectorImpl omHaMetricsCollector = new MetricsCollectorImpl();
    om.getOmhaMetrics().getMetrics(omHaMetricsCollector, true);
    assertEquals(omRaftGroups, omHaInfoRaftGroupsLeaders.size());
    Assertions.assertEquals(omRaftGroups + 1, omHaMetricsCollector.getRecords().size());
  }

  private void writeKey(ClientProtocol ozoneClient, String volume, String bucket, String key) throws IOException {
    writeKey(ozoneClient, volume, bucket, key, key);
  }

  private void writeKey(ClientProtocol ozoneClient, String volume, String bucket, String key, String fileText)
          throws IOException {
    try (OzoneOutputStream stream = ozoneClient.createKey(
            volume, bucket, key, key.length(), ReplicationConfig.getDefault(conf), Collections.emptyMap())
    ) {
      stream.write(fileText.getBytes(UTF_8));
    }
  }


  private static void waitOmRaftGroupsSizeOnNodesEqual(OzoneManager om1, OzoneManager om2, OzoneManager om3,
                                                       int expectedGroupSize)
      throws TimeoutException, InterruptedException {
    waitFor(
        () ->
            om1.getOmRaftGroups().size() == expectedGroupSize &&
                om2.getOmRaftGroups().size() == expectedGroupSize &&
                om3.getOmRaftGroups().size() == expectedGroupSize,
        1000,
        120000
    );
  }

  private static void waitOneOfOmRaftGroupsSizeOnNodesLess(OzoneManager om1, OzoneManager om2, OzoneManager om3,
                                                           int expectedGroupSize)
      throws TimeoutException, InterruptedException {
    waitFor(
        () ->
            om1.getOmRaftGroups().size() < expectedGroupSize ||
                om2.getOmRaftGroups().size() < expectedGroupSize ||
                om3.getOmRaftGroups().size() < expectedGroupSize,
        1000,
        80000
    );
  }

  private void waitMultiRaftTerm(int expectedTerm) throws TimeoutException, InterruptedException {
    waitFor(
        () -> {
          OzoneManager omLeader = cluster.getOMLeader();
          if (omLeader == null) {
            return false;
          } else {
            return omLeader.getCurrentMultiRaftTerm() == expectedTerm;
          }
        },
        1000,
        80000
    );
  }

  private void waitLeaderElection() throws TimeoutException, InterruptedException {
    waitFor(
        () -> cluster.getOMLeader() != null, 100,
        (int) Duration.of(80, ChronoUnit.SECONDS).toMillis()
    );
  }

  private void checkRemovedDirs(OzoneManager om, UUID mainGroupUuid, List<String> dirsListBefore) {
    List<String> dirsListAfter = getDirsList(om.getConfiguration());
    assertTrue(dirsListAfter.contains(mainGroupUuid.toString()));
    dirsListAfter.removeAll(dirsListBefore);
    assertEquals(4, dirsListAfter.size());
  }

  private List<String> getDirsList(OzoneConfiguration configuration) {
    String omRatisDirectory = ServerUtils.getDefaultRatisDirectory(configuration, HddsProtos.NodeType.OM);
    File ratisMetadataDir = new File(omRatisDirectory);
    if (ratisMetadataDir.exists()) {
      String[] list = ratisMetadataDir.list();

      return new ArrayList<>(Arrays.asList(list == null ? new String[]{} : list));
    }
    return Lists.emptyList();
  }

  private void checkKeyReading(String volume, String bucket, String key) throws IOException {
    try (
        OzoneInputStream ozoneInputStream = cluster.createClient().getProxy().getKey(volume, bucket, key);
        BufferedReader br = new BufferedReader(new InputStreamReader(ozoneInputStream, UTF_8));
    ) {

      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
      String result = sb.toString();
      assertEquals(key, result);
    }
  }

  private void checkKeyReading(String volume, String bucket, String key, String expectedText) throws IOException {
    try (
        OzoneInputStream ozoneInputStream = cluster.createClient().getProxy().getKey(volume, bucket, key);
        BufferedReader br = new BufferedReader(new InputStreamReader(ozoneInputStream, UTF_8));
    ) {

      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
      String result = sb.toString();
      assertEquals(expectedText, result);
    }
  }

  private boolean isKeyEquals(String volume, String bucket, String key, String expectedText) {
    try (
        OzoneInputStream ozoneInputStream = cluster.createClient().getProxy().getKey(volume, bucket, key);
        BufferedReader br = new BufferedReader(new InputStreamReader(ozoneInputStream, UTF_8));
    ) {

      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
      String result = sb.toString();
      return expectedText.equals(result);
    } catch (Exception e) {
      LOG.error("Error checking key reading", e);
      return false;
    }
  }

  private long getKeyUpdateId(String volume, String bucket, String key) throws IOException {
    OzoneKeyDetails ozoneKeyDetails = cluster.createClient().getProxy().getKeyDetails(volume, bucket, key);
    return ozoneKeyDetails.getUpdateId();
  }

  private static void waitTermIndex(OzoneManager om, long expectedIndex)
      throws TimeoutException, InterruptedException {
    waitFor(
        () -> om.getStateMachines().values().stream().filter(BucketStateMachine.class::isInstance)
            .findFirst().map(StateMachine::getLastAppliedTermIndex)
            .map(TermIndex::getIndex)
            .orElse(0L) == expectedIndex, 100, 120000);
  }

  private static void checkLastAppliedIndex(Long expectedIndex, OzoneManager om) {
    Long lastAppliedTermIndex =
        om.getStateMachines().values().stream().filter(BucketStateMachine.class::isInstance)
            .findFirst().map(StateMachine::getLastAppliedTermIndex)
            .map(TermIndex::getIndex)
            .orElse(0L);
    assertEquals(expectedIndex, lastAppliedTermIndex);
  }
}
