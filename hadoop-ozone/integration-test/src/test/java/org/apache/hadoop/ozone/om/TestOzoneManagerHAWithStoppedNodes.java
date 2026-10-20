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

package org.apache.hadoop.ozone.om;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT;
import static org.apache.ozone.test.OzoneTestBase.uniqueObjectName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.MockJobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.SendHeartbeatRequest;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdfs.LogVerificationAppender;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.JobworkerService;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.jobworker.JobworkerConnectionManager;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.apache.hadoop.ozone.jobworker.commands.AbstractJobworkerCommandHandler;
import org.apache.hadoop.ozone.jobworker.commands.JobworkerCommand;
import org.apache.hadoop.ozone.jobworker.commands.JobworkerCommandHandler;
import org.apache.hadoop.ozone.jobworker.commands.OMJobworkerCommand;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ha.OMHAMetrics;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.jobworker.command.JobworkerCommandListener;
import org.apache.hadoop.ozone.om.jobworker.command.JobworkerJobState;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.om.service.KeyDeletingService;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.JobworkerJobRecord;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

/**
 * Ozone Manager HA tests that stop/restart one or more OM nodes.
 * @see TestOzoneManagerHAWithAllRunning
 */

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestOzoneManagerHAWithStoppedNodes extends OzoneManagerHATests {
  @TempDir
  private Path jobworkerDirectory;
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      TestOzoneManagerHAWithStoppedNodes.class);

  static {
    setExtraClusterConfig(c -> c.set(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, "2s"));
  }

  /**
   * After restarting OMs we need to wait
   * for a leader to be elected and ready.
   */
  @BeforeEach
  void setup() throws Exception {
    waitForLeaderToBeReady();
  }

  @Test
  void testJobworkerRecoversRunningAttemptAndLostCompletionAcknowledgement() throws Exception {
    MiniOzoneHAClusterImpl cluster = getCluster();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(1);
    AtomicInteger executions = new AtomicInteger();
    AtomicBoolean rejectAcknowledgement = new AtomicBoolean();
    JobworkerService worker = new JobworkerService(new String[0]) {
      @Override
      protected JobworkerCommandHandler[] createCommandHandlers() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AbstractJobworkerCommandHandler handler =
            new AbstractJobworkerCommandHandler(
                OMJobworkerCommandProto.Type.mockCommand, executor) {
              @Override
              protected boolean processCommand(JobworkerCommand<?> command,
                  JobworkerStateContext context,
                  JobworkerConnectionManager connections) throws Exception {
                executions.incrementAndGet();
                started.countDown();
                return finish.await(120, TimeUnit.SECONDS);
              }

              @Override
              public void stop() {
                ServerUtils.executorServiceShutdownGraceful(executor);
              }
            };
        return new JobworkerCommandHandler[]{handler};
      }
    };
    try {
      OzoneConfiguration workerConf = enableJobworkerFramework(rejectAcknowledgement);
      OzoneManager firstLeader = cluster.getOMLeader();
      worker.start(workerConf);
      UUID workerId = worker.getJobworkerDetails().getUuid();
      GenericTestUtils.waitFor(
          () -> firstLeader.getJobworkerNodemanager().isJobworkerNodeRegistered(workerId), 100, 30_000);
      OMJobworkerCommandProto envelope =
          OMJobworkerCommandProto
              .newBuilder().setCommandType(OMJobworkerCommandProto.Type.mockCommand).setCmdId(42).build();
      firstLeader.getOMJobworkerCommandManager().sendCommand(workerId,
          OMJobworkerCommand.fromEnvelope(envelope));
      assertTrue(started.await(30, TimeUnit.SECONDS));
      String jobKey = JobworkerJobState.key(envelope.getCommandType(), 42);
      GenericTestUtils.waitFor(() -> jobworkerHasStatus(firstLeader, jobKey,
          CommandStatus.Status.EXECUTING),
          100, 30_000);

      cluster.stopOzoneManager(firstLeader.getOMNodeId());
      waitForLeaderToBeReady();
      OzoneManager secondLeader = cluster.getOMLeader();
      assertNotEquals(firstLeader.getOMNodeId(), secondLeader.getOMNodeId());
      assertTrue(jobworkerHasStatus(secondLeader, jobKey,
          CommandStatus.Status.EXECUTING));
      assertEquals(1, executions.get(), "An OM election must not restart an executing attempt");

      // Restore quorum capacity before the second failure.
      cluster.restartOzoneManager(firstLeader, true);
      registerJobworkerResultListener(firstLeader, rejectAcknowledgement);
      rejectAcknowledgement.set(true);
      finish.countDown();
      GenericTestUtils.waitFor(() -> jobworkerHasStatus(secondLeader, jobKey,
          CommandStatus.Status.SUCCEEDED),
          100, 30_000);
      assertNotNull(worker.getJobworkerStateMachine().getContext().getCommandManager()
          .getCmdStatus(secondLeader.getOMServiceId(), 42L),
          "A socket response must not retire an unacknowledged result");

      // The result is committed, but its application acknowledgement has not reached the worker.
      cluster.stopOzoneManager(secondLeader.getOMNodeId());
      waitForLeaderToBeReady();
      OzoneManager thirdLeader = cluster.getOMLeader();
      rejectAcknowledgement.set(false);
      GenericTestUtils.waitFor(() -> worker.getJobworkerStateMachine().getContext().getCommandManager()
          .getCmdStatus(thirdLeader.getOMServiceId(), 42L) == null, 100, 30_000);
      assertTrue(jobworkerHasStatus(thirdLeader, jobKey,
          CommandStatus.Status.SUCCEEDED));
      assertEquals(1, executions.get());

      createVolumeTest(true);
      createKeyTest(true);
      cluster.restartOzoneManager();
      waitForLeaderToBeReady();
      assertTrue(jobworkerHasStatus(cluster.getOMLeader(), jobKey,
          CommandStatus.Status.SUCCEEDED));
      assertEquals(1, executions.get(), "Committed work must survive a full OM restart without executing twice");
      worker.stop();
      worker.start(workerConf);
      assertEquals(workerId, worker.getJobworkerDetails().getUuid());
      GenericTestUtils.waitFor(() -> worker.getJobworkerStateMachine().getConnectionManager().getAllEndpoints().stream()
          .allMatch(endpoint -> endpoint.getState() == org.apache.hadoop.ozone.jobworker.JobworkerEndpointStateMachine
              .EndpointStates.HEARTBEAT), 100, 30_000);
      OMJobworkerCommandProto oldDelivery = OMJobworkerCommandProto.parseFrom(
          cluster.getOMLeader().getJobworkerJobManager().get(jobKey).getCommand());
      worker.getJobworkerStateMachine().getContext().getCommandManager()
          .acceptDurableCommand(oldDelivery, cluster.getOMLeader().getOMServiceId(), 100);
      assertEquals(1, executions.get(), "Worker restart and delayed delivery must not repeat acknowledged effects");
    } finally {
      finish.countDown();
      worker.stop();
      for (OzoneManager om : cluster.getOzoneManagersList()) {
        om.getConfiguration().setBoolean("ozone.om.jobworker.service.enabled", false);
      }
    }
  }

  private OzoneConfiguration enableJobworkerFramework(AtomicBoolean rejectAcknowledgement) throws Exception {
    MiniOzoneHAClusterImpl cluster = getCluster();
    cluster.restartOzoneManagersWithConfigCustomizer(conf -> {
      conf.setBoolean("ozone.om.jobworker.service.enabled", true);
      conf.setInt("ozone.om.jobworker.grpc.port", 0);
    });
    OzoneConfiguration workerConf = new OzoneConfiguration(getConf());
    for (OzoneManager om : cluster.getOzoneManagersList()) {
      for (OzoneManager endpoint : cluster.getOzoneManagersList()) {
        String key = "ozone.om.jobworker.grpc.port." + endpoint.getOMServiceId() + "." + endpoint.getOMNodeId();
        om.getConfiguration().setInt(key, endpoint.getJobworkerGrpcPort());
        workerConf.setInt(key, endpoint.getJobworkerGrpcPort());
      }
      registerJobworkerResultListener(om, rejectAcknowledgement);
    }
    workerConf.set("ozone.jobworker.om.service.ids", cluster.getOMLeader().getOMServiceId());
    workerConf.set("ozone.jobworker.metadata.dir", jobworkerDirectory.toString());
    workerConf.set("ozone.jobworker.heartbeat.interval", "100ms");
    workerConf.set("ozone.jobworker.command.status.report.interval", "100ms");
    workerConf.set("ozone.jobworker.node.report.interval", "100ms");
    return workerConf;
  }

  @Test
  void testJobworkerDoesNotDispatchAnUncommittedAssignmentWithoutQuorum() throws Exception {
    MiniOzoneHAClusterImpl cluster = getCluster();
    ExecutorService submitter = Executors.newSingleThreadExecutor();
    try {
      enableJobworkerFramework(new AtomicBoolean());
      OzoneManager leader = cluster.getOMLeader();
      JobworkerDetails worker =
          MockJobworkerDetails.randomJobworkerDetails();
      RegisterJobworkerRequest register =
          RegisterJobworkerRequest
              .newBuilder().setDurableJobProtocolVersion(1)
              .setExtendedJobWorkDetailsProto(worker.getExtendedProtoBufMessage()).build();
      leader.getJobworkerServerProtocol().registerJobworker(register);
      OMJobworkerCommandProto committed = OMJobworkerCommandProto.newBuilder()
          .setCommandType(OMJobworkerCommandProto.Type.mockCommand).setCmdId(142).build();
      leader.getOMJobworkerCommandManager().sendCommand(worker.getUuid(), OMJobworkerCommand.fromEnvelope(committed));
      List<OzoneManager> followers = new ArrayList<>(cluster.getOzoneManagersList());
      followers.remove(leader);
      followers.forEach(om -> cluster.stopOzoneManager(om.getOMNodeId()));
      long committedIndex = leader.getOmRatisServer().getServerDivision().getRaftLog().getLastCommittedIndex();
      long nextIndex = leader.getOmRatisServer().getServerDivision().getRaftLog().getNextIndex();
      OMJobworkerCommandProto uncommitted = committed.toBuilder().setCmdId(143).build();
      Future<Exception> submission = submitter.submit(() -> {
        try {
          leader.getOMJobworkerCommandManager().sendCommand(worker.getUuid(),
              OMJobworkerCommand.fromEnvelope(uncommitted));
          return null;
        } catch (IOException e) {
          return e;
        }
      });
      GenericTestUtils.waitFor(() -> submission.isDone()
          || leader.getOmRatisServer().getServerDivision().getRaftLog().getNextIndex() > nextIndex, 50, 10_000);
      if (submission.isDone()) {
        assertNotNull(submission.get(), "A new assignment cannot commit with only one of three OMs");
      }
      assertEquals(committedIndex, leader.getOmRatisServer().getServerDivision().getRaftLog().getLastCommittedIndex());
      assertNull(leader.getJobworkerJobManager().get(JobworkerJobState.key(uncommitted.getCommandType(), 143)));
      SendHeartbeatRequest heartbeat =
          SendHeartbeatRequest.newBuilder()
              .setOmServiceId(leader.getOMServiceId()).setJobworkerDetails(worker.getProtoBufMessage()).build();
      try {
        assertTrue(leader.getJobworkerServerProtocol().sendHeartbeat(heartbeat).getCommandsList().stream()
            .noneMatch(command -> command.getCmdId() == 143));
      } catch (IOException e) {
        // Leadership may be lost between the readiness check and report reconciliation.
        assertTrue(e.getMessage().contains("ready OM leader"), "Only a leadership refusal is expected: " + e);
      }
      assertNull(leader.getJobworkerJobManager().get(JobworkerJobState.key(uncommitted.getCommandType(), 143)));

      for (OzoneManager follower : followers) {
        cluster.restartOzoneManager(follower, true);
        registerJobworkerResultListener(follower, new AtomicBoolean());
      }
      waitForLeaderToBeReady();
      Exception firstAttempt = submission.get(30, TimeUnit.SECONDS);
      if (firstAttempt != null) {
        assertTrue(firstAttempt instanceof IOException);
      }
      OzoneManager recovered = cluster.getOMLeader();
      // Whether the original request committed after recovery or failed, retrying the stable ID is safe.
      recovered.getOMJobworkerCommandManager().sendCommand(worker.getUuid(),
          OMJobworkerCommand.fromEnvelope(uncommitted));
      assertNotNull(recovered.getJobworkerJobManager().get(JobworkerJobState.key(uncommitted.getCommandType(), 143)));
    } finally {
      submitter.shutdownNow();
      for (OzoneManager om : cluster.getOzoneManagersList()) {
        om.getConfiguration().setBoolean("ozone.om.jobworker.service.enabled", false);
      }
    }
  }

  private static boolean jobworkerHasStatus(OzoneManager om, String key,
      CommandStatus.Status expected) {
    try {
      JobworkerJobRecord record =
          om.getJobworkerJobManager().get(key);
      return record != null && CommandStatus.parseFrom(record.getStatus()).getStatus() == expected;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void registerJobworkerResultListener(OzoneManager om,
      AtomicBoolean rejectAcknowledgement) {
    JobworkerCommandListener listener =
        org.mockito.Mockito.mock(JobworkerCommandListener.class);
    org.mockito.Mockito.doCallRealMethod().when(listener).decodeCommand(org.mockito.ArgumentMatchers.any());
    org.mockito.Mockito.doAnswer(invocation -> {
      if (rejectAcknowledgement.get()) {
        throw new IllegalStateException("Simulated lost result acknowledgement");
      }
      return null;
    }).when(listener).onCommandSucceeded(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(),
        org.mockito.ArgumentMatchers.any());
    om.getOMJobworkerCommandManager().registerHandler(
        OMJobworkerCommandProto.Type.mockCommand, listener);
  }

  /**
   * Restart all OMs after each test.
   */
  @AfterEach
  void resetCluster() throws Exception {
    MiniOzoneHAClusterImpl cluster = getCluster();
    if (cluster != null) {
      cluster.restartOzoneManager();
    }
  }

  /**
   * Test client request succeeds when one OM node is down.
   */
  @Test
  void oneOMDown() throws Exception {
    getCluster().stopOzoneManager(1);
    waitForLeaderToBeReady();

    createVolumeTest(true);
    createKeyTest(true);
  }

  /**
   * Test client request fails when 2 OMs are down.
   */
  @Test
  void twoOMDown() throws Exception {
    getCluster().stopOzoneManager(1);
    getCluster().stopOzoneManager(2);

    createVolumeTest(false);
    createKeyTest(false);
  }

  @Test
  void testMultipartUpload() throws Exception {

    // Happy scenario when all OM's are up.
    OzoneBucket ozoneBucket = setupBucket();

    String keyName = UUID.randomUUID().toString();
    String uploadID = initiateMultipartUpload(ozoneBucket, keyName);

    createMultipartKeyAndReadKey(ozoneBucket, keyName, uploadID);

    testMultipartUploadWithOneOmNodeDown();
  }

  private void testMultipartUploadWithOneOmNodeDown() throws Exception {

    OzoneBucket ozoneBucket = setupBucket();

    String keyName = UUID.randomUUID().toString();
    String uploadID = initiateMultipartUpload(ozoneBucket, keyName);

    // After initiate multipartupload, shutdown leader OM.
    // Stop leader OM, to see when the OM leader changes
    // multipart upload is happening successfully or not.

    final HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> omFailoverProxyProvider
        = OmTestUtil.getFailoverProxyProvider(getObjectStore());

    // The omFailoverProxyProvider will point to the current leader OM node.
    String leaderOMNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    // Stop one of the ozone manager, to see when the OM leader changes
    // multipart upload is happening successfully or not.
    getCluster().stopOzoneManager(leaderOMNodeId);
    waitForLeaderToBeReady();

    createMultipartKeyAndReadKey(ozoneBucket, keyName, uploadID);

    String newLeaderOMNodeId =
        omFailoverProxyProvider.getCurrentProxyOMNodeId();

    assertNotEquals(leaderOMNodeId, newLeaderOMNodeId);
  }

  private String initiateMultipartUpload(OzoneBucket ozoneBucket,
      String keyName) throws Exception {

    OmMultipartInfo omMultipartInfo =
        ozoneBucket.initiateMultipartUpload(keyName,
            ReplicationType.RATIS,
            ReplicationFactor.ONE);

    String uploadID = omMultipartInfo.getUploadID();
    assertNotNull(uploadID);
    return uploadID;
  }

  private void createMultipartKeyAndReadKey(OzoneBucket ozoneBucket,
      String keyName, String uploadID) throws Exception {

    String value = "random data";
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createMultipartKey(
        keyName, value.length(), 1, uploadID);
    ozoneOutputStream.write(value.getBytes(UTF_8), 0, value.length());
    ozoneOutputStream.getMetadata().put(OzoneConsts.ETAG, DigestUtils.md5Hex(value));
    ozoneOutputStream.close();


    Map<Integer, String> partsMap = new HashMap<>();
    partsMap.put(1, ozoneOutputStream.getCommitUploadPartInfo().getETag());
    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo =
        ozoneBucket.completeMultipartUpload(keyName, uploadID, partsMap);

    assertNotNull(omMultipartUploadCompleteInfo);
    assertNotNull(omMultipartUploadCompleteInfo.getHash());


    try (OzoneInputStream ozoneInputStream = ozoneBucket.readKey(keyName)) {
      byte[] fileContent = new byte[value.getBytes(UTF_8).length];
      IOUtils.readFully(ozoneInputStream, fileContent);
      assertEquals(value, new String(fileContent, UTF_8));
    }
  }

  /**
   * Test HadoopRpcOMFailoverProxyProvider failover on connection exception
   * to OM client.
   */
  @Test
  public void testOMProxyProviderFailoverOnConnectionFailure()
      throws Exception {
    ObjectStore objectStore = getObjectStore();
    final HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> omFailoverProxyProvider
        = OmTestUtil.getFailoverProxyProvider(objectStore);

    createVolumeTest(true);
    String firstProxyNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    // On stopping the current OM Proxy, the next connection attempt should
    // failover to a another OM proxy.
    getCluster().stopOzoneManager(firstProxyNodeId);
    waitForLeaderToBeReady();

    // Next request to the proxy provider should result in a failover
    createVolumeTest(true);
    GenericTestUtils.waitFor(
        () -> !firstProxyNodeId.equals(omFailoverProxyProvider.getCurrentProxyOMNodeId()),
        100, (int) (OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT * 5));

    // Get the new OM Proxy NodeId
    String newProxyNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    // Verify that a failover occurred. the new proxy nodeId should be
    // different from the old proxy nodeId.
    assertNotEquals(firstProxyNodeId, newProxyNodeId);
  }

  @Test
  @Order(Integer.MAX_VALUE)
  void testOMRestart() throws Exception {
    // start fresh cluster
    shutdown();
    init();

    ObjectStore objectStore = getObjectStore();
    // Get the leader OM
    final String leaderOMNodeId = OmTestUtil.getCurrentOmProxyNodeId(objectStore);
    OzoneManager leaderOM = getCluster().getOzoneManager(leaderOMNodeId);

    // Get follower OM
    OzoneManager followerOM1 = getCluster().getOzoneManager(
        leaderOM.getPeerNodes().get(0).getNodeId());

    // Do some transactions so that the log index increases
    String userName = "user" + RandomStringUtils.secure().nextNumeric(5);
    String adminName = "admin" + RandomStringUtils.secure().nextNumeric(5);
    String volumeName = uniqueObjectName("volume");
    String bucketName = uniqueObjectName("bucket");

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

    objectStore.createVolume(volumeName, createVolumeArgs);
    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);

    ozoneVolume.createBucket(bucketName);
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);

    for (int i = 0; i < 10; i++) {
      createKey(ozoneBucket);
    }

    final long followerOM1LastAppliedIndex =
        followerOM1.getOmRatisServer().getLastAppliedTermIndex().getIndex();

    // Stop one follower OM
    followerOM1.stop();

    // Do more transactions. Stopped OM should miss these transactions and
    // the logs corresponding to at least some missed transactions
    // should be purged. This will force the OM to install snapshot when
    // restarted.
    long minNewTxIndex = followerOM1LastAppliedIndex + getLogPurgeGap() * 10L;
    while (leaderOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
        < minNewTxIndex) {
      createKey(ozoneBucket);
    }

    // Get the latest snapshotIndex from the leader OM.
    final long leaderOMSnaphsotIndex = leaderOM.getRatisSnapshotIndex();

    // The stopped OM should be lagging behind the leader OM.
    assertThat(followerOM1LastAppliedIndex).isLessThan(leaderOMSnaphsotIndex);

    // Restart the stopped OM.
    followerOM1.restart();

    // Wait for the follower OM to catch up
    GenericTestUtils.waitFor(() -> followerOM1.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex() >= leaderOMSnaphsotIndex,
        100, 200000);

    // Do more transactions. The restarted OM should receive the
    // new transactions. It's last applied tx index should increase from the
    // last snapshot index after more transactions are applied.
    for (int i = 0; i < 10; i++) {
      createKey(ozoneBucket);
    }

    GenericTestUtils.waitFor(() -> followerOM1.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex() > leaderOMSnaphsotIndex,
        100, 200000);

    final long followerOM1LastAppliedIndexNew =
        followerOM1.getOmRatisServer().getLastAppliedTermIndex().getIndex();
    assertThat(followerOM1LastAppliedIndexNew).isGreaterThan(leaderOMSnaphsotIndex);
  }

  @Test
  void testListParts() throws Exception {

    OzoneBucket ozoneBucket = setupBucket();
    String keyName = UUID.randomUUID().toString();
    String uploadID = initiateMultipartUpload(ozoneBucket, keyName);

    Map<Integer, String> partsMap = new HashMap<>();
    partsMap.put(1, createMultipartUploadPartKey(ozoneBucket, 1, keyName,
        uploadID));
    partsMap.put(2, createMultipartUploadPartKey(ozoneBucket, 2, keyName,
        uploadID));
    partsMap.put(3, createMultipartUploadPartKey(ozoneBucket, 3, keyName,
        uploadID));

    validateListParts(ozoneBucket, keyName, uploadID, partsMap);

    // Stop leader OM, and then validate list parts.
    stopLeaderOM();
    waitForLeaderToBeReady();

    validateListParts(ozoneBucket, keyName, uploadID, partsMap);

  }

  /**
   * Validate parts uploaded to a MPU Key.
   */
  private void validateListParts(OzoneBucket ozoneBucket, String keyName,
      String uploadID, Map<Integer, String> partsMap) throws Exception {
    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        ozoneBucket.listParts(keyName, uploadID, 0, 1000);

    List<OzoneMultipartUploadPartListParts.PartInfo> partInfoList =
        ozoneMultipartUploadPartListParts.getPartInfoList();

    assertEquals(partInfoList.size(), partsMap.size());

    for (int i = 0; i < partsMap.size(); i++) {
      assertEquals(partsMap.get(partInfoList.get(i).getPartNumber()),
          partInfoList.get(i).getETag());

    }

    assertFalse(ozoneMultipartUploadPartListParts.isTruncated());
  }

  /**
   * Create a Multipart upload part Key with specified partNumber and uploadID.
   * @return Part name for the uploaded part.
   */
  private String createMultipartUploadPartKey(OzoneBucket ozoneBucket,
      int partNumber, String keyName, String uploadID) throws Exception {
    String value = "random data";
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createMultipartKey(
        keyName, value.length(), partNumber, uploadID);
    ozoneOutputStream.write(value.getBytes(UTF_8), 0, value.length());
    ozoneOutputStream.getMetadata().put(OzoneConsts.ETAG, DigestUtils.md5Hex(value));
    ozoneOutputStream.close();

    return ozoneOutputStream.getCommitUploadPartInfo().getETag();
  }

  @Test
  public void testConf() {
    final RaftProperties p = getCluster()
        .getOzoneManager()
        .getOmRatisServer()
        .getServerDivision()
        .getRaftServer()
        .getProperties();
    final TimeDuration t = RaftServerConfigKeys.Log.Appender.waitTimeMin(p);
    assertEquals(TimeDuration.ZERO, t,
        RaftServerConfigKeys.Log.Appender.WAIT_TIME_MIN_KEY);
  }

  @Test
  public void testKeyDeletion() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String data = "random data";
    String keyName1 = "dir/file1";
    String keyName2 = "dir/file2";
    String keyName3 = "dir/file3";
    String keyName4 = "dir/file4";

    testCreateFile(ozoneBucket, keyName1, data, true, false);
    testCreateFile(ozoneBucket, keyName2, data, true, false);
    testCreateFile(ozoneBucket, keyName3, data, true, false);
    testCreateFile(ozoneBucket, keyName4, data, true, false);

    ozoneBucket.deleteKey(keyName1);
    ozoneBucket.deleteKey(keyName2);
    ozoneBucket.deleteKey(keyName3);
    ozoneBucket.deleteKey(keyName4);

    // Now check delete table has entries been removed.

    OzoneManager ozoneManager = getCluster().getOMLeader();

    final KeyDeletingService keyDeletingService = ozoneManager.getKeyManager().getDeletingService();

    // Check on leader OM Count.
    GenericTestUtils.waitFor(() ->
        keyDeletingService.getRunCount().get() >= 2, 1000, 120000);
    GenericTestUtils.waitFor(() ->
        keyDeletingService.getDeletedKeyCount().get() == 4, 1000, 120000);

    // Check delete table is empty or not on all OMs.
    getCluster().getOzoneManagersList().forEach((om) -> {
      try {
        GenericTestUtils.waitFor(() -> {
          Table<String, RepeatedOmKeyInfo> deletedTable =
              om.getMetadataManager().getDeletedTable();
          try (Table.KeyValueIterator<String, RepeatedOmKeyInfo> iterator = deletedTable.iterator()) {
            return !iterator.hasNext();
          } catch (Exception ex) {
            return false;
          }
        },
            1000, 120000);
      } catch (Exception ex) {
        fail("TestOzoneManagerHAKeyDeletion failed");
      }
    });
  }

  /**
   * 1. Stop one of the OM
   * 2. make a call to OM, this will make failover attempts to find new node.
   * a) if LE finishes but leader not ready, it retries to same node
   * b) if LE not done, it will failover to new node and check
   * 3. Try failover to same OM explicitly.
   * Now #3 should wait additional waitBetweenRetries time.
   * LE: Leader Election.
   */
  @Test
  @Order(Integer.MAX_VALUE - 1)
  void testIncrementalWaitTimeWithSameNodeFailover() throws Exception {
    long waitBetweenRetries = getConf().getLong(
        OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_KEY,
        OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT);
    final HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> omFailoverProxyProvider
        = OmTestUtil.getFailoverProxyProvider(getObjectStore());

    // The omFailoverProxyProvider will point to the current leader OM node.
    String leaderOMNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    getCluster().stopOzoneManager(leaderOMNodeId);
    getCluster().waitForLeaderOM();
    createKeyTest(true); // failover should happen to new node

    long numTimesTriedToSameNode = omFailoverProxyProvider.getWaitTime()
        / waitBetweenRetries;
    omFailoverProxyProvider.setNextOmProxy(omFailoverProxyProvider.
        getCurrentProxyOMNodeId());
    assertEquals((numTimesTriedToSameNode + 1) * waitBetweenRetries,
        omFailoverProxyProvider.getWaitTime());
  }

  @Test
  void testOMHAMetrics() throws Exception {
    // Get leader OM
    OzoneManager leaderOM = getCluster().getOMLeader();
    // Store current leader's node ID,
    // to use it after restarting the OM
    String leaderOMId = leaderOM.getOMNodeId();
    // Get a list of all OMs
    List<OzoneManager> omList = getCluster().getOzoneManagersList();
    // Check metrics for all OMs
    checkOMHAMetricsForAllOMs(omList, leaderOMId);

    // Restart leader OM
    getCluster().shutdownOzoneManager(leaderOM);
    getCluster().restartOzoneManager(leaderOM, true);
    waitForLeaderToBeReady();

    // Do some writes so that the old leader can receive AppendEntries
    // which will trigger notifyLeaderChanged, instead of relying on
    // AppendEntries
    setupBucket();

    // Get the new leader
    OzoneManager newLeaderOM = getCluster().getOMLeader();
    String newLeaderOMId = newLeaderOM.getOMNodeId();
    // Get a list of all OMs again
    omList = getCluster().getOzoneManagersList();
    // New state for the old leader
    int newState = leaderOMId.equals(newLeaderOMId) ? 1 : 0;

    // Get old leader
    OzoneManager oldLeader = getCluster().getOzoneManager(leaderOMId);
    // Get old leader's metrics
    OMHAMetrics omhaMetrics = oldLeader.getOmhaMetrics();

    assertEquals(newState,
        omhaMetrics.getOmhaInfoOzoneManagerHALeaderState());

    // Check that metrics for all OMs have been updated
    checkOMHAMetricsForAllOMs(omList, newLeaderOMId);
  }

  private void checkOMHAMetricsForAllOMs(List<OzoneManager> omList,
      String leaderOMId) {
    for (OzoneManager om : omList) {
      // Get OMHAMetrics for the current OM
      OMHAMetrics omhaMetrics = om.getOmhaMetrics();
      String nodeId = om.getOMNodeId();

      // If current OM is leader, state should be 1
      int expectedState = nodeId
          .equals(leaderOMId) ? 1 : 0;
      assertEquals(expectedState,
          omhaMetrics.getOmhaInfoOzoneManagerHALeaderState());

      assertEquals(nodeId, omhaMetrics.getOmhaInfoNodeId());
    }
  }

  @Test
  void testOMRetryProxy() {
    int maxFailoverAttempts = getOzoneClientFailoverMaxAttempts();
    // Stop all the OMs.
    for (int i = 0; i < getNumOfOMs(); i++) {
      getCluster().stopOzoneManager(i);
    }

    final LogVerificationAppender appender = new LogVerificationAppender();
    final Logger logger = Logger.getRootLogger();
    logger.addAppender(appender);

    // After making N (set maxRetries value) connection attempts to OMs,
    // the RpcClient should give up.
    assertThrows(ConnectException.class, () -> createVolumeTest(true));
    assertEquals(1,
        appender.countLinesWithMessage("Failed to connect to OMs:"));
    assertEquals(maxFailoverAttempts,
        appender.countLinesWithMessage("Trying to failover"));
    assertEquals(1, appender.countLinesWithMessage("Attempted " +
        maxFailoverAttempts + " failovers."));
  }

  @Test
  void testListVolumes() throws Exception {
    String userName = UserGroupInformation.getCurrentUser().getUserName();
    ObjectStore objectStore = getObjectStore();

    String prefix = uniqueObjectName("vol-") + "-";
    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(userName)
        .build();

    Set<String> expectedVolumes = new TreeSet<>();
    for (int i = 0; i < 100; i++) {
      String volumeName = prefix + i;
      expectedVolumes.add(volumeName);
      objectStore.createVolume(volumeName, createVolumeArgs);
    }

    validateVolumesList(expectedVolumes,
        objectStore.listVolumesByUser(userName, prefix, ""));

    // Stop leader OM, and then validate list volumes for user.
    stopLeaderOM();
    waitForLeaderToBeReady();

    validateVolumesList(expectedVolumes,
        objectStore.listVolumesByUser(userName, prefix, ""));
  }

  @Test
  void testRetryCacheWithDownedOM() throws Exception {
    // Create a volume, a bucket and a key
    String userName = "user" + RandomStringUtils.secure().nextNumeric(5);
    String adminName = "admin" + RandomStringUtils.secure().nextNumeric(5);
    String volumeName = uniqueObjectName("volume");
    String bucketName = UUID.randomUUID().toString();
    String keyTo = UUID.randomUUID().toString();

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();
    getObjectStore().createVolume(volumeName, createVolumeArgs);
    OzoneVolume ozoneVolume = getObjectStore().getVolume(volumeName);
    ozoneVolume.createBucket(bucketName);
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    String keyFrom = createKey(ozoneBucket);

    int callId = 10;
    ClientId clientId = ClientId.randomId();
    MiniOzoneHAClusterImpl cluster = getCluster();
    OzoneManager omLeader = cluster.getOMLeader();

    OzoneManagerProtocolProtos.KeyArgs keyArgs =
        OzoneManagerProtocolProtos.KeyArgs.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyFrom)
            .build();
    OzoneManagerProtocolProtos.RenameKeyRequest renameKeyRequest
        = OzoneManagerProtocolProtos.RenameKeyRequest.newBuilder()
        .setKeyArgs(keyArgs)
        .setToKeyName(keyTo)
        .build();
    OzoneManagerProtocolProtos.OMRequest omRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.RenameKey)
            .setRenameKeyRequest(renameKeyRequest)
            .setClientId(clientId.toString())
            .build();
    // set up the current call so that OM Ratis Server doesn't complain.
    Server.getCurCall().set(new Server.Call(callId, 0, null, null,
        RPC.RpcKind.RPC_BUILTIN, clientId.toByteString().toByteArray()));
    // Submit rename request to OM
    OzoneManagerProtocolProtos.OMResponse omResponse =
        omLeader.getOmServerProtocol().processRequest(omRequest);
    assertTrue(omResponse.getSuccess());

    // Make one of the follower OM the leader, and shutdown the current leader.
    OzoneManager newLeader = cluster.getOzoneManagersList().stream().filter(
        om -> !om.getOMNodeId().equals(omLeader.getOMNodeId())).findFirst().get();
    transferLeader(omLeader, newLeader);
    cluster.shutdownOzoneManager(omLeader);

    // Once the rename completes, the source key should no longer exist
    // and the destination key should exist.
    OMException omException = assertThrows(OMException.class,
        () -> ozoneBucket.getKey(keyFrom));
    assertEquals(omException.getResult(), OMException.ResultCodes.KEY_NOT_FOUND);
    assertTrue(ozoneBucket.getKey(keyTo).isFile());

    // Submit rename request to OM again. The request is cached so it will succeed.
    omResponse = newLeader.getOmServerProtocol().processRequest(omRequest);
    assertTrue(omResponse.getSuccess());
  }

  private void transferLeader(OzoneManager omLeader, OzoneManager newLeader) throws IOException {
    LOG.info("Transfer leadership from {}(raft id {}) to {}(raft id {})",
        omLeader.getOMNodeId(), omLeader.getOmRatisServer().getRaftPeerId(),
        newLeader.getOMNodeId(), newLeader.getOmRatisServer().getRaftPeerId());

    final SupportedRpcType rpc = SupportedRpcType.GRPC;
    final RaftProperties properties = RatisHelper.newRaftProperties(rpc);

    // For now not making anything configurable, RaftClient  is only used
    // in SCM for DB updates of sub-ca certs go via Ratis.
    RaftClient.Builder builder = RaftClient.newBuilder()
        .setRaftGroup(omLeader.getOmRatisServer().getRaftGroup())
        .setLeaderId(null)
        .setProperties(properties)
        .setRetryPolicy(
            RetryPolicies.retryUpToMaximumCountWithFixedSleep(120,
                TimeDuration.valueOf(500, TimeUnit.MILLISECONDS)));
    try (RaftClient raftClient = builder.build()) {
      RaftClientReply reply = raftClient.admin().transferLeadership(newLeader.getOmRatisServer()
          .getRaftPeerId(), 10 * 1000);
      assertTrue(reply.isSuccess());
    }
  }

  private void validateVolumesList(Set<String> expectedVolumes,
      Iterator<? extends OzoneVolume> volumeIterator) {
    int expectedCount = 0;

    while (volumeIterator.hasNext()) {
      OzoneVolume next = volumeIterator.next();
      assertThat(expectedVolumes).contains(next.getName());
      expectedCount++;
    }

    assertEquals(expectedVolumes.size(), expectedCount);
  }

}
