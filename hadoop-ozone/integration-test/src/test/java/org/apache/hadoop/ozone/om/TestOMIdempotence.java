package org.apache.hadoop.ozone.om;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY;
import static org.junit.Assert.assertTrue;

/**
 * Test the idempotence of OM operations.
 * On restarts, OM Ratis could replay already applied transactions. Hence,
 * all OM write operations should be idempotent.
 */
public class TestOMIdempotence {

  private MiniOzoneHAClusterImpl cluster = null;
  private ObjectStore objectStore;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omServiceId;
  private int numOfOMs = 3;

  /* Reduce max number of retries to speed up unit test. */
  private static final int OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS = 5;
  private static final int IPC_CLIENT_CONNECT_MAX_RETRIES = 4;
  private static final String USER_NAME = "user12345";
  private static final String ALT_USER_NAME = "user6789";
  private static final String ADMIN_NAME = "admin12345";
  private static final String clientId = UUID.randomUUID().toString();

  @Rule
  public Timeout timeout = new Timeout(300_000);

  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omServiceId = "om-service-test1";
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.setBoolean(OZONE_OM_RATIS_ENABLE_KEY, true);
    conf.set(OzoneConfigKeys.OZONE_ADMINISTRATORS,
        OZONE_ADMINISTRATORS_WILDCARD);
    conf.setInt(OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS, 2);
    conf.setInt(OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
        OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS);
    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        IPC_CLIENT_CONNECT_MAX_RETRIES);
    /* Reduce IPC retry interval to speed up unit test. */
    conf.setInt(IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY, 200);
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(numOfOMs)
        .build();
    cluster.waitForClusterToBeReady();
    objectStore = OzoneClientFactory.getRpcClient(omServiceId, conf)
        .getObjectStore();
  }

  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * An extension of {@link AuditLogger} to capture audit logs. These logs
   * are used to verify execution of operations.
   */
  public class AuditLoggerForTesting extends AuditLogger {

    private List<Map<String, String>> parsedAuditLogs = new ArrayList<>();

    /**
     * Parametrized Constructor to initialize logger.
     *
     * @param type Audit Logger Type
     */
    AuditLoggerForTesting(AuditLoggerType type) {
      super(type);
    }

    @Override
    public void logWrite(AuditMessage auditMessage) {
      super.logWrite(auditMessage);
      Throwable exception = auditMessage.getThrowable();
      if (exception != null) {
        Map<String, String> params = parseAuditMessage(
            auditMessage.getFormattedMessage(), exception);
        parsedAuditLogs.add(params);
      }
    }

    /**
     * Parse the audit log message for errors and param values.
     */
    private Map<String, String> parseAuditMessage(String message,
        Throwable exception) {
      Map<String, String> params = new HashMap<>();

      if (exception instanceof OMException) {
        OMException.ResultCodes result =
            ((OMException) exception).getResult();
        params.put("error", result.name());
      } else {
        params.put("error", exception.getMessage());
      }

      message = message.replace(" | ", " ")
          .replace("{", "")
          .replace("}", "")
          .replace(",", "");
      String[] keyValuePairs = message.split(" ");

      for (String keyValuePair : keyValuePairs) {
        if (keyValuePair.contains("=")) {
          String[] keyAndValue = keyValuePair.split("=");
          params.put(keyAndValue[0], keyAndValue[1]);
        }
      }
      return params;
    }

    /**
     * Return the audit logs which have a param matching the given key and
     * value.
     */
    List<String> getAuditLogErrorsForKeyValue(String key,
        String value) {
      List<String> errorMsgs = new ArrayList<>();
      for (Map<String, String> params : parsedAuditLogs) {
        if (params.containsKey(key) && params.get(key).equals(value)) {
          errorMsgs.add(params.get("error"));
        }
      }
      return errorMsgs;
    }
  }

  /**
   * Get the Leader OM.
   */
  private OzoneManager getLeaderOM() {
    // Get the leader OM
    String leaderOMNodeId = objectStore.getClientProxy().getOMProxyProvider()
        .getCurrentProxyOMNodeId();
    return cluster.getOzoneManager(leaderOMNodeId);
  }

  /**
   * Get one of the follower OM.
   */
  private OzoneManager getFollowerOM(OzoneManager leaderOM) {
    // Get one of the follower OMs
    OzoneManager followerOM = cluster.getOzoneManager(
        leaderOM.getPeerNodes().get(0).getOMNodeId());

    return followerOM;
  }

  /**
   * Get the current TermIndex on the given OM.
   */
  private TermIndex getCurrentTermIndex(OzoneManager om) {
    // Get the current termIndex of the OM.
    return om.getOmRatisServer().getOmStateMachine().getLastAppliedTermIndex();
  }

  /**
   * Set the custom AuditLoggerForTesting as the AuditLogger on the given OM.
   * Also, force set the LastAppliedTermIndex of the OM to given TermIndex.
   */
  private AuditLoggerForTesting setupFollowerOMForAuditLogging(
      OzoneManager om, TermIndex initialTermIndex)
      throws Exception {
    // Force set the lastAppliedIndex on follower OM to initial TermIndex
    TermIndex lastAppliedTermIndex =
        om.getOmRatisServer().getLastAppliedTermIndex();
    om.getOmRatisServer().getOmStateMachine()
        .forceSetLastAppliedTermIndex(initialTermIndex);
    om.stop();

    // Set AuditLogger for testing before restarting OM
    AuditLoggerForTesting auditLog =
        new AuditLoggerForTesting(AuditLoggerType.OMLOGGER);
    om.setAuditLogger(auditLog);

    om.restart();

    // Wait for follower to catch up with Leader by replaying the logs.
    GenericTestUtils.waitFor(() ->
            om.getOmRatisServer().getOmStateMachine()
                .getLastAppliedTermIndex().getIndex() >=
                lastAppliedTermIndex.getIndex(),
        100, 100000);

    return auditLog;
  }

  /**
   * Assert that the captured audit logs contain the expected error message.
   */
  private void assertExpectedErrorMsg(AuditLoggerForTesting auditLogger,
      String key, String value, String errorMsg) {
    List<String> auditLogErrorMsgs =
        auditLogger.getAuditLogErrorsForKeyValue(key, value);
    for (String auditLogErrorMsg : auditLogErrorMsgs) {
      if (errorMsg.equals(auditLogErrorMsg)) {
        return;
      }
    }
    assertTrue("Expected error message was not logged", false);
  }

  /**
   * Test idempotence of CreateVolume, DeleteVolume, SetOwner and
   * SetQuota requests.
   */
  @Test
  public void testVolumeOperationsIdempotence() throws Exception {
    // Get a follower OM
    OzoneManager leaderOM = getLeaderOM();
    OzoneManager followerOM = getFollowerOM(leaderOM);
    // Get the initial termIndex of the follower OM.
    TermIndex initialTermIndex = getCurrentTermIndex(followerOM);

    // Perform the operations to test idempotence for.
    String volume1Name = "volume" + RandomStringUtils.randomNumeric(5);
    String volume2Name = "volume" + RandomStringUtils.randomNumeric(5);

    // CreateVolume operation
    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(USER_NAME)
        .setAdmin(ADMIN_NAME)
        .build();
    objectStore.createVolume(volume1Name, createVolumeArgs);
    objectStore.createVolume(volume2Name, createVolumeArgs);
    OzoneVolume volume1Info = objectStore.getVolume(volume1Name);

    // SetOwner and SetQuota operations
    volume1Info.setOwner(ALT_USER_NAME);
    volume1Info.setQuota(OzoneQuota.parseQuota("500MB"));

    // DeleteVolume operation
    objectStore.deleteVolume(volume2Name);

    // Setup AuditLogger for testing idempotence of above operations
    AuditLoggerForTesting auditLog =
        setupFollowerOMForAuditLogging(followerOM, initialTermIndex);

    // Verify audit log has the expected error/ message after replaying the
    // transactions.
    assertExpectedErrorMsg(auditLog, "volume", volume1Name,
        OMException.ResultCodes.VOLUME_ALREADY_EXISTS.toString());

    // Verify that the OM state is as expected
    OMMetadataManager leaderMetaMngr = leaderOM.getMetadataManager();
    OMMetadataManager followerMetaMngr = followerOM.getMetadataManager();

    // Verify Volume information is consistent with leader OM
    String dbVolume1Key = leaderMetaMngr.getVolumeKey(volume1Name);
    OmVolumeArgs dbVolume1ArgsOnLeader = leaderMetaMngr.getVolumeTable()
        .get(dbVolume1Key);
    OmVolumeArgs dbVolume1ArgsOnFollower = followerMetaMngr.getVolumeTable()
        .get(dbVolume1Key);
    Assert.assertEquals("Create Volume operation is not idempotent",
        dbVolume1ArgsOnLeader.toAuditMap(),
        dbVolume1ArgsOnFollower.toAuditMap());

    String dbVolume2Key = followerMetaMngr.getVolumeKey(volume2Name);
    boolean volume2Exists = followerMetaMngr.getVolumeTable()
        .isExist(dbVolume2Key);
    Assert.assertFalse(volume2Exists);
  }

  /**
   * Test idempotence of CreateBucket and DeleteBucket.
   */
  @Test
  public void testBucketOperationsIdempotence() throws Exception {
    // Get a follower OM
    OzoneManager leaderOM = getLeaderOM();
    OzoneManager followerOM = getFollowerOM(leaderOM);
    // Get the initial termIndex of the follower OM.
    TermIndex initialTermIndex = getCurrentTermIndex(followerOM);

    // Perform the operations to test idempotence for.
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucket1Name = "bucket" + RandomStringUtils.randomNumeric(5);
    String bucket2Name = "bucket" + RandomStringUtils.randomNumeric(5);

    // CreateVolume operation
    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(USER_NAME)
        .setAdmin(ADMIN_NAME)
        .build();
    objectStore.createVolume(volumeName, createVolumeArgs);
    OzoneVolume volumeInfo = objectStore.getVolume(volumeName);

    // CreateBucket operation
    volumeInfo.createBucket(bucket1Name);
    volumeInfo.createBucket(bucket2Name);

    // DeleteBucket operation
    volumeInfo.deleteBucket(bucket2Name);

    // Setup AuditLogger for testing idempotence of above operations
    AuditLoggerForTesting auditLog =
        setupFollowerOMForAuditLogging(followerOM, initialTermIndex);

    // Verify audit log has the expected error/ message after replaying the
    // transactions.
    assertExpectedErrorMsg(auditLog, "bucket", bucket1Name,
        OMException.ResultCodes.BUCKET_ALREADY_EXISTS.toString());

    // Verify that the OM state is as expected
    OMMetadataManager leaderMetaMngr = leaderOM.getMetadataManager();
    OMMetadataManager followerMetaMngr = followerOM.getMetadataManager();

    // Verify Bucket information is consistent with leader OM
    String dbBucket1Key = leaderMetaMngr.getBucketKey(volumeName, bucket1Name);
    OmBucketInfo omBucket1InfoOnLeader = leaderMetaMngr.getBucketTable()
        .get(dbBucket1Key);
    OmBucketInfo omBucket1InfoOnFollower = followerMetaMngr.getBucketTable()
        .get(dbBucket1Key);
    Assert.assertEquals("Create Bucket operation is not idempotent",
        omBucket1InfoOnLeader.toAuditMap(),
        omBucket1InfoOnFollower.toAuditMap());

    String dbBucket2Key = leaderMetaMngr.getBucketKey(volumeName, bucket2Name);
    boolean bucket2Exists = followerMetaMngr.getBucketTable()
        .isExist(dbBucket2Key);
    Assert.assertFalse(bucket2Exists);
  }
}
