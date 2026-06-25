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

package org.apache.hadoop.ozone.om.service;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_TENANT_RANGER_POLICY_LABEL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMMultiTenantManager.OZONE_TENANT_RANGER_ROLE_DESCRIPTION;
import static org.apache.hadoop.ozone.om.OMMultiTenantManagerImpl.OZONE_OM_TENANT_DEV_SKIP_RANGER;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult.EmptyTaskResult;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.OMMultiTenantManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.om.multitenant.AuthorizerLock;
import org.apache.hadoop.ozone.om.multitenant.InMemoryMultiTenantAccessController;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Policy;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Role;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetRangerServiceVersionRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background Sync thread that reads Multi-Tenancy state from OM DB
 * and applies it to Ranger. This recovers or cleans up (Multi-Tenant related)
 * Ranger policies and roles in case of OM crashes or Ranger failure.
 *
 * Multi-Tenant related Ranger policies and roles are *eventually* consistent
 * with OM DB tenant state. OM DB is the source of truth.
 *
 * While the sync thread is updating Ranger, user or other applications
 * editing Ranger Ozone policies or roles could interfere with the update.
 * In this case, a sync run might leave Ranger in a de-synced state, due to
 * limited maximum number of update attempts for each run.
 * But this should eventually be corrected in future sync runs.
 *
 * See the comment block in triggerRangerSyncOnce() for more on the core logic.
 */
public class OMRangerBGSyncService extends BackgroundService {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMRangerBGSyncService.class);
  private static final ClientId CLIENT_ID = ClientId.randomId();

  private final OzoneManager ozoneManager;
  private final OMMetadataManager metadataManager;
  private final OMMultiTenantManager multiTenantManager;
  private final MultiTenantAccessController accessController;
  private final AuthorizerLock authorizerLock;

  // Maximum number of attempts for each sync run
  private static final int MAX_ATTEMPT = 2;
  private final AtomicLong runCount = new AtomicLong(0);

  private volatile boolean isServiceStarted = false;

  // This map keeps all the policies found in OM DB. These policies should be
  // in Ranger. If not, the default policy will be (re)created.
  //
  // Maps from policy name to PolicyInfo (tenant name and policy type) in Ranger
  private final HashMap<String, PolicyInfo> mtRangerPoliciesToBeCreated =
      new HashMap<>();

  // We will track all the policies in Ranger here. After we have
  // processed all the policies from OM DB, this map will
  // be left with policies that we need to delete.
  //
  // Maps from policy name to policy ID in Ranger
  private final HashMap<String, String> mtRangerPoliciesToBeDeleted =
      new HashMap<>();

  // This map will keep all the Multi-Tenancy related roles from Ranger.
  private final HashMap<String, BGRole> mtRangerRoles = new HashMap<>();

  // Keep OM DB mapping of Roles -> list of user principals.
  private final HashMap<String, HashSet<String>> mtOMDBRoles = new HashMap<>();

  public OMRangerBGSyncService(OzoneManager ozoneManager,
      OMMultiTenantManager omMultiTenantManager,
      MultiTenantAccessController accessController,
      long interval, TimeUnit unit, long serviceTimeout) {

    super("OMRangerBGSyncService", interval, unit, 1, serviceTimeout,
        ozoneManager.getThreadNamePrefix());

    this.ozoneManager = ozoneManager;
    this.metadataManager = ozoneManager.getMetadataManager();
    // Note: ozoneManager.getMultiTenantManager() may return null because
    // it might haven't finished initialization.
    this.multiTenantManager = omMultiTenantManager;
    this.authorizerLock = omMultiTenantManager.getAuthorizerLock();

    if (accessController != null) {
      this.accessController = accessController;
    } else {
      // authorizer can be null for unit tests
      LOG.warn("MultiTenantAccessController not set. "
          + "Using in-memory controller.");
      this.accessController = new InMemoryMultiTenantAccessController();
    }
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new RangerBGSyncTask());
    return queue;
  }

  @Override
  public void start() {
    if (accessController == null) {
      LOG.error("Failed to start the background sync service: "
          + "null authorizer. Please check OM configuration. Aborting");
      return;
    }
    isServiceStarted = true;
    super.start();
  }

  @Override
  public void shutdown() {
    isServiceStarted = false;
    super.shutdown();
  }

  /**
   * Returns true if the service run conditions are satisfied, false otherwise.
   */
  private boolean shouldRun() {
    if (ozoneManager == null) {
      // OzoneManager can be null for testing
      return true;
    }
    if (ozoneManager.getOmRatisServer() == null) {
      LOG.warn("OzoneManagerRatisServer is not initialized yet");
      return false;
    }
    // The service only runs if current OM node is leader and is ready
    //  and the service is marked as started
    return isServiceStarted && ozoneManager.isLeaderReady();
  }

  private class RangerBGSyncTask implements BackgroundTask {

    @Override
    public int getPriority() {
      return 0;
    }

    @Override
    public BackgroundTaskResult call() {
      // Check OM leader and readiness
      if (shouldRun()) {
        final long count = runCount.incrementAndGet();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Initiating Multi-Tenancy Ranger Sync: run # {}", count);
        }
        triggerRangerSyncOnce();
      }

      return EmptyTaskResult.newResult();
    }
  }

  /**
   * Trigger the sync once.
   * @return true if completed successfully, false if any exception is thrown.
   */
  public synchronized boolean triggerRangerSyncOnce() {
    int attempt = 0;
    try {
      long dbOzoneServiceVersion = getOMDBRangerServiceVersion();
      long rangerOzoneServiceVersion = getRangerOzoneServicePolicyVersion();

      // Sync thread enters the while-loop when Ranger service (e.g. cm_ozone)
      // version doesn't match the current service version persisted in the DB.
      //
      // When Ranger state related to the Ozone service (e.g. policies or roles)
      // is updated, it bumps up the service version.
      // At the end of the loop, Ranger service version is retrieved again.
      // So in this case the loop most likely be entered a second time. But
      // this time it is very likely that executeOMDBToRangerSync() won't push
      // policy or role updates to Ranger as it should already be in-sync from
      // the previous loop. If this is the case, the DB service version will be
      // written again, and the loop should be exited.
      //
      // If Ranger service version is bumped again, it indicates that Ranger
      // roles or policies were updated by ongoing OM multi-tenancy requests,
      // or manually by a user.
      //
      // A maximum of MAX_ATTEMPT times will be attempted each time the sync
      // service is run. MAX_ATTEMPT should at least be 2 to make sure OM DB
      // has the up-to-date Ranger service version most of the times.
      while (dbOzoneServiceVersion != rangerOzoneServiceVersion) {

        if (++attempt > MAX_ATTEMPT) {
          LOG.warn("Reached maximum number of attempts ({}). Abort",
              MAX_ATTEMPT);
          break;
        }

        LOG.info("Executing Multi-Tenancy Ranger Sync: run # {}, attempt # {}. "
                + "Ranger service version: {}, DB service version: {}",
            runCount.get(), attempt,
            rangerOzoneServiceVersion, dbOzoneServiceVersion);

        executeOMDBToRangerSync(dbOzoneServiceVersion);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Setting DB Ranger service version to {} (was {})",
              rangerOzoneServiceVersion, dbOzoneServiceVersion);
        }
        // Submit Ratis Request to sync the new service version in OM DB
        setOMDBRangerServiceVersion(rangerOzoneServiceVersion);

        // Check Ranger Ozone service version again
        dbOzoneServiceVersion = rangerOzoneServiceVersion;
        rangerOzoneServiceVersion = getRangerOzoneServicePolicyVersion();
      }
    } catch (IOException | ServiceException e) {
      LOG.warn("Exception during Ranger Sync", e);
      return false;
    } finally {
      if (attempt > 0) {
        LOG.info("Finished executing Multi-Tenancy Ranger Sync run # {} after" +
            "{} attempts.", runCount.get(), attempt);
      }
    }

    return true;
  }

  /**
   * Query Ranger endpoint to get the latest Ozone service version.
   */
  long getRangerOzoneServicePolicyVersion() throws IOException {
    long policyVersion = accessController.getRangerServicePolicyVersion();
    if (policyVersion < 0L) {
      LOG.warn("Unable to get valid policyVersion for Ranger background sync "
              + "to function properly. Please check if the Kerberos principal "
              + "as configured in ozone.om.kerberos.principal ({}) has admin "
              + "privilege in Ranger.",
          ozoneManager.getConfiguration().get(OZONE_OM_KERBEROS_PRINCIPAL_KEY));
    }
    return policyVersion;
  }

  public void setOMDBRangerServiceVersion(long version)
          throws ServiceException {
    // OM DB update goes through Ratis
    SetRangerServiceVersionRequest.Builder versionSyncRequest =
        SetRangerServiceVersionRequest.newBuilder()
            .setRangerServiceVersion(version);

    OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(Type.SetRangerServiceVersion)
        .setSetRangerServiceVersionRequest(versionSyncRequest)
        .setClientId(CLIENT_ID.toString())
        .build();

    try {
      OzoneManagerRatisUtils.submitRequest(ozoneManager, omRequest, CLIENT_ID, runCount.get());
    } catch (ServiceException e) {
      LOG.error("SetRangerServiceVersion request failed. "
          + "Will retry at next run.", e);
      throw e;
    }
  }

  long getOMDBRangerServiceVersion() throws IOException {
    final String dbValue = ozoneManager.getMetadataManager().getMetaTable()
        .get(OzoneConsts.RANGER_OZONE_SERVICE_VERSION_KEY);
    if (dbValue == null) {
      return -1L;
    } else {
      return Long.parseLong(dbValue);
    }
  }

  private void executeOMDBToRangerSync(long baseVersion) throws IOException {
    clearPolicyAndRoleMaps();


    withOptimisticRead(() -> {
      try {
        loadAllPoliciesAndRoleNamesFromRanger(baseVersion);
        loadAllRolesFromRanger();
        loadAllRolesFromOM();
      } catch (IOException e) {
        LOG.error("Failed to load policies or roles from Ranger or DB", e);
        throw new RuntimeException(e);
      }
    });

    // This should isolate policies into two groups
    // 1. mtRangerPoliciesTobeDeleted and
    // 2. mtRangerPoliciesTobeCreated
    processAllPoliciesFromOMDB();

    // This should isolate roles that need fixing into a list of
    // roles that need to be replayed back into ranger to get in sync with OMDB.
    processAllRolesFromOMDB();
  }

  private void clearPolicyAndRoleMaps() {
    mtRangerPoliciesToBeCreated.clear();
    mtRangerPoliciesToBeDeleted.clear();
    mtRangerRoles.clear();
    mtOMDBRoles.clear();
  }

  private List<Policy> getAllMultiTenantPolicies() throws IOException {

    return accessController.getLabeledPolicies(
        OZONE_TENANT_RANGER_POLICY_LABEL);
  }

  private void loadAllPoliciesAndRoleNamesFromRanger(long baseVersion)
      throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("baseVersion is {}", baseVersion);
    }

    final List<Policy> allPolicies = getAllMultiTenantPolicies();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Received policies with {} label: {}",
          OZONE_TENANT_RANGER_POLICY_LABEL, allPolicies);
    }

    if (allPolicies.isEmpty()) {
      // This is normal only if no tenant is created yet.
      LOG.info("No Ranger policy with label {} received.",
          OZONE_TENANT_RANGER_POLICY_LABEL);
      return;
    }

    for (Policy policy : allPolicies) {

      // Verify that the policy has the OzoneTenant label
      if (!policy.getLabels().contains(OZONE_TENANT_RANGER_POLICY_LABEL)) {
        // Shouldn't get policies without the label very often as it is
        // specified in the query param, unless someone removed the tag during
        // the get all policies request.
        LOG.warn("Ignoring Ranger policy without the {} label: {}",
            OZONE_TENANT_RANGER_POLICY_LABEL, policy);
        continue;
      }

      // Temporarily put the policy in the to-delete list,
      // valid entries will be removed later
      mtRangerPoliciesToBeDeleted.put(policy.getName(),
          String.valueOf(policy.getId()));

      // We don't need to care about policy.getUserAcls() in the sync as
      // we only uses special {OWNER} user, at least for now.

      // Iterate through all the roles in this policy
      policy.getRoleAcls().keySet().forEach(roleName -> {
        if (!mtRangerRoles.containsKey(roleName)) {
          // We only got role name here. Will check users in the roles later.
          mtRangerRoles.put(roleName, new BGRole(roleName));
        }
      });
    }

  }

  /**
   * Helper function to throw benign exception if the current OM is no longer
   * the leader in case a leader transition happened during the sync. So the
   * sync run can abort earlier.
   *
   * Note: EACH Ranger request can take 3-7 seconds as tested in UT.
   */
  private void checkLeader() throws IOException {
    if (ozoneManager.getConfiguration().getBoolean(
        OZONE_OM_TENANT_DEV_SKIP_RANGER, false)) {
      // Skip leader check if the test flag is set, used in TestOzoneTenantShell
      // TODO: Find a proper fix in MiniOzoneCluster to pass
      //  ozoneManager.isLeaderReady() check?
      return;
    }
    if (!ozoneManager.isLeaderReady()) {
      throw new OMNotLeaderException(
          ozoneManager.getOmRatisServer().getRaftPeerId());
    }
  }

  private void loadAllRolesFromRanger() throws IOException {
    for (Map.Entry<String, BGRole> entry: mtRangerRoles.entrySet()) {
      final String roleName = entry.getKey();
      checkLeader();
      final Role role = accessController.getRole(roleName);
      final BGRole bgRole = entry.getValue();
      bgRole.setId(role.getName());
      for (String username : role.getUsersMap().keySet()) {
        bgRole.addUserPrincipal(username);
      }
    }
  }

  /**
   * Helper function to add/remove a policy name to/from mtRangerPolicies lists.
   */
  private void mtRangerPoliciesOpHelper(
      String policyName, PolicyInfo policyInfo) {

    if (mtRangerPoliciesToBeDeleted.containsKey(policyName)) {
      // This entry is in sync with Ranger, remove it from the set
      // Eventually mtRangerPolicies will only contain entries that
      // are not in OMDB and should be removed from Ranger.
      mtRangerPoliciesToBeDeleted.remove(policyName);
    } else {
      // We could not find a policy in ranger that should have been there.
      mtRangerPoliciesToBeCreated.put(policyName, policyInfo);
    }
  }

  /**
   * Helper function to retry the block until it completes without a write lock
   * being acquired during its execution. The block will be retried
   * {@link this#MAX_ATTEMPT} times.
   */
  private void withOptimisticRead(Runnable block) throws IOException {
    // Acquire a stamp that will be used to check if a write occurred while we
    // were reading.
    // If a tenant modification is made while we are reading,
    // retry the read operation with a new stamp until we are able to read the
    // state without a write operation interrupting.
    int attempt = 0;
    boolean readSuccess = false;
    while (!readSuccess && attempt < MAX_ATTEMPT) {
      long stamp = authorizerLock.tryOptimisticReadThrowOnTimeout();
      block.run();
      readSuccess = authorizerLock.validateOptimisticRead(stamp);
      attempt++;
    }

    if (!readSuccess) {
      throw new IOException("Failed to read state for Ranger background sync" +
          " without an interrupting write operation after " + attempt +
          " attempts.");
    }
  }

  /**
   * Helper function to run the block with write lock held.
   */
  private void withWriteLock(Runnable block) throws IOException {
    // Acquire authorizer (Ranger) write lock
    long stamp = authorizerLock.tryWriteLockThrowOnTimeout();
    try {
      block.run();
    } finally {
      // Release authorizer (Ranger) write lock
      authorizerLock.unlockWrite(stamp);
    }
  }

  private void processAllPoliciesFromOMDB() throws IOException {

    // Iterate all DB tenant states. For each tenant,
    // queue or dequeue bucketNamespacePolicyName and bucketPolicyName
    try (TableIterator<String, ? extends KeyValue<String, OmDBTenantState>>
        tenantStateTableIt = metadataManager.getTenantStateTable().iterator()) {

      while (tenantStateTableIt.hasNext()) {
        final KeyValue<String, OmDBTenantState> tableKeyValue =
            tenantStateTableIt.next();
        final OmDBTenantState dbTenantState = tableKeyValue.getValue();
        final String tenantId = dbTenantState.getTenantId();
        final String volumeName = dbTenantState.getBucketNamespaceName();
        Objects.requireNonNull(volumeName, "volumeName == null");

        mtRangerPoliciesOpHelper(dbTenantState.getBucketNamespacePolicyName(),
            new PolicyInfo(tenantId, PolicyType.BUCKET_NAMESPACE_POLICY));
        mtRangerPoliciesOpHelper(dbTenantState.getBucketPolicyName(),
            new PolicyInfo(tenantId, PolicyType.BUCKET_POLICY));
      }
    }

    for (Map.Entry<String, PolicyInfo> entry :
        mtRangerPoliciesToBeCreated.entrySet()) {
      final String policyName = entry.getKey();
      LOG.warn("Expected policy not found in Ranger: {}", policyName);
      checkLeader();
      // Attempt to recreate the default volume/bucket policy if it's missing
      attemptToCreateDefaultPolicy(entry.getValue());
    }

    for (Map.Entry<String, String> entry :
        mtRangerPoliciesToBeDeleted.entrySet()) {
      final String policyName = entry.getKey();
      LOG.info("Deleting policy from Ranger: {}", policyName);
      checkLeader();
      withWriteLock(() -> {
        try {
          accessController.deletePolicy(policyName);
        } catch (IOException e) {
          LOG.error("Failed to delete policy: {}", policyName, e);
          // Proceed to delete other policies
        }
      });
    }

  }

  /**
   * Attempts to (re)create a tenant default volume or bucket policy in Ranger.
   */
  private void attemptToCreateDefaultPolicy(PolicyInfo policyInfo)
      throws IOException {

    final String tenantId = policyInfo.getTenantId();

    final String volumeName =
        multiTenantManager.getTenantVolumeName(tenantId);
    final String userRoleName =
        multiTenantManager.getTenantUserRoleName(tenantId);

    final Policy accessPolicy;

    switch (policyInfo.getPolicyType()) {
    case BUCKET_NAMESPACE_POLICY:
      LOG.info("Recovering VolumeAccess policy for tenant: {}", tenantId);

      final String adminRoleName =
          multiTenantManager.getTenantAdminRoleName(tenantId);

      accessPolicy = OMMultiTenantManager.getDefaultVolumeAccessPolicy(
          tenantId, volumeName, userRoleName, adminRoleName);
      break;

    case BUCKET_POLICY:
      LOG.info("Recovering BucketAccess policy for tenant: {}", tenantId);

      accessPolicy = OMMultiTenantManager.getDefaultBucketAccessPolicy(
          tenantId, volumeName, userRoleName);
      break;

    default:
      throw new OMException("Unknown policy type in " + policyInfo,
          ResultCodes.INTERNAL_ERROR);
    }

    withWriteLock(() -> {
      try {
        final Policy policy = accessController.createPolicy(accessPolicy);
        LOG.info("Created policy: {}", policy);
      } catch (IOException e) {
        LOG.error("Failed to create policy: {}", accessPolicy, e);
      }
    });
  }

  private void loadAllRolesFromOM() throws IOException {
    if (multiTenantManager instanceof OMMultiTenantManagerImpl) {
      loadAllRolesFromCache();
    } else {
      LOG.warn("Cache is not supported for {}. Loading roles directly from DB",
          multiTenantManager.getClass().getSimpleName());
      loadAllRolesFromDB();
    }
  }

  private void loadAllRolesFromCache() {

    final OMMultiTenantManagerImpl impl =
        (OMMultiTenantManagerImpl) multiTenantManager;

    mtOMDBRoles.putAll(impl.getAllRolesFromCache());
  }

  private void loadAllRolesFromDB() throws IOException {
    // We have the following in OM DB
    //  tenantStateTable: tenantId -> TenantState (has user and admin role name)
    //  tenantAccessIdTable : accessId -> OmDBAccessIdInfo

    final Table<String, OmDBTenantState> tenantStateTable =
        metadataManager.getTenantStateTable();

    // Iterate all DB ExtendedUserAccessIdInfo. For each accessId,
    // add to userRole. And add to adminRole if isAdmin is set.
    try (TableIterator<String, ? extends KeyValue<String, OmDBAccessIdInfo>>
        tenantAccessIdTableIter =
        metadataManager.getTenantAccessIdTable().iterator()) {

      while (tenantAccessIdTableIter.hasNext()) {
        final KeyValue<String, OmDBAccessIdInfo> tableKeyValue =
            tenantAccessIdTableIter.next();
        final OmDBAccessIdInfo dbAccessIdInfo = tableKeyValue.getValue();

        final String tenantId = dbAccessIdInfo.getTenantId();
        final String userPrincipal = dbAccessIdInfo.getUserPrincipal();

        final OmDBTenantState dbTenantState = tenantStateTable.get(tenantId);

        if (dbTenantState == null) {
          // tenant state might have been deleted by some other ongoing requests
          LOG.warn("OmDBTenantState for tenant '{}' doesn't exist!", tenantId);
          continue;
        }

        final String userRoleName = dbTenantState.getUserRoleName();
        mtOMDBRoles.computeIfAbsent(userRoleName, any -> new HashSet<>());
        final String adminRoleName = dbTenantState.getAdminRoleName();
        mtOMDBRoles.computeIfAbsent(adminRoleName, any -> new HashSet<>());

        // Every tenant user should be in the tenant's userRole
        addUserToMtOMDBRoles(userRoleName, userPrincipal);

        // If the accessId has admin flag set, add to adminRole as well
        if (dbAccessIdInfo.getIsAdmin()) {
          addUserToMtOMDBRoles(adminRoleName, userPrincipal);
        }
      }
    }
  }

  /**
   * Helper function to add user principal to a role in mtOMDBRoles.
   */
  private void addUserToMtOMDBRoles(String roleName, String userPrincipal) {
    if (!mtOMDBRoles.containsKey(roleName)) {
      mtOMDBRoles.put(roleName, new HashSet<>(
          Collections.singletonList(userPrincipal)));
    } else {
      final HashSet<String> usersInTheRole = mtOMDBRoles.get(roleName);
      usersInTheRole.add(userPrincipal);
    }
  }

  private void processAllRolesFromOMDB() throws IOException {
    // Lets First make sure that all the Roles in OM DB are present in Ranger
    // as well as the corresponding userlist matches matches.
    for (Map.Entry<String, HashSet<String>> entry : mtOMDBRoles.entrySet()) {
      final String roleName = entry.getKey();
      boolean pushRoleToRanger = false;
      if (mtRangerRoles.containsKey(roleName)) {
        final HashSet<String> rangerUserList =
            mtRangerRoles.get(roleName).getUserSet();
        final HashSet<String> userSet = entry.getValue();
        for (String userPrincipal : userSet) {
          if (rangerUserList.contains(userPrincipal)) {
            rangerUserList.remove(userPrincipal);
          } else {
            // We found a user in OM DB Role that doesn't exist in Ranger Role.
            // Lets just push the role from OM DB to Ranger
            pushRoleToRanger = true;
            break;
          }
        }
        // We have processed all the Userlist entries in the OMDB. If
        // ranger Userlist is not empty, Ranger has users that OM DB does not.
        if (!rangerUserList.isEmpty()) {
          pushRoleToRanger = true;
        }
      } else {
        // 1. The role is missing from Ranger, or;
        // 2. A policy (that uses this role) is missing from Ranger, causing
        // mtRangerRoles to be populated incorrectly. In this case the roles
        // are there just fine. If not, will be corrected in the next run anyway
        checkLeader();
        withWriteLock(() -> {
          try {
            Role role = new Role.Builder()
                .setName(roleName)
                .setDescription(OZONE_TENANT_RANGER_ROLE_DESCRIPTION)
                .build();
            accessController.createRole(role);
          } catch (IOException e) {
            // Tolerate create role failure, possibly due to role already exists
            LOG.error("Failed to create role: {}", roleName, e);
          }
        });
        pushRoleToRanger = true;
      }
      if (pushRoleToRanger) {
        LOG.info("Updating role in Ranger: {}", roleName);
        checkLeader();
        pushOMDBRoleToRanger(roleName);
      }
      // We have processed this role in OM DB now. Lets remove it from
      // mtRangerRoles. Eventually whatever is left in mtRangerRoles
      // are extra entries, that should not have been in Ranger.
      mtRangerRoles.remove(roleName);
    }

    // A hack (for now) to delete UserRole before AdminRole
    final Set<String> rolesToDelete = new TreeSet<>(Collections.reverseOrder());
    rolesToDelete.addAll(mtRangerRoles.keySet());

    for (String roleName : rolesToDelete) {
      LOG.warn("Deleting role from Ranger: {}", roleName);
      checkLeader();
      withWriteLock(() -> {
        try {
          accessController.deleteRole(roleName);
        } catch (IOException e) {
          // The role might have been deleted already.
          // Or the role could be referenced in other roles or policies.
          LOG.error("Failed to delete role: {}", roleName);
        }
      });
      // TODO: Server returned HTTP response code: 400
      //  if already deleted or is depended on
    }
  }

  private void pushOMDBRoleToRanger(String roleName) throws IOException {
    final HashSet<String> omDBUserList = mtOMDBRoles.get(roleName);
    withWriteLock(() -> {
      try {
        Role existingRole = accessController.getRole(roleName);
        if (!existingRole.getId().isPresent()) {
          // Should not happen. getRole() would have thrown exception if
          // role doesn't exist in Ranger.
          LOG.error("Role doesn't have ID: {}", existingRole);
          return;
        }
        long roleId = existingRole.getId().get();
        Role newRole = new Role.Builder(existingRole)
            .clearUsers()
            .addUsers(omDBUserList)
            .build();
        // TODO: Double check result
        accessController.updateRole(roleId, newRole);
      } catch (IOException e) {
        LOG.error("Failed to update role: {}, target user list: {}",
            roleName, omDBUserList, e);
      }
    });
  }

  /**
   * Return the number of runs the sync is triggered.
   *
   * This doesn't count attempts inside each sync run.
   */
  public long getRangerSyncRunCount() {
    return runCount.get();
  }

  static class BGRole {
    private final String name;
    private String id;
    private final HashSet<String> userSet;

    BGRole(String n) {
      this.name = n;
      userSet = new HashSet<>();
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getId() {
      return id;
    }

    public void addUserPrincipal(String userPrincipal) {
      userSet.add(userPrincipal);
    }

    public HashSet<String> getUserSet() {
      return userSet;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, id, userSet);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      BGRole bgRole = (BGRole) o;
      return name.equals(bgRole.name)
                 && id.equals(bgRole.id)
                 && userSet.equals(bgRole.userSet);
    }
  }

  enum PolicyType {
    BUCKET_NAMESPACE_POLICY,
    BUCKET_POLICY
  }

  /**
   * Helper class that stores the tenant name and policy type.
   */
  static class PolicyInfo {

    private final String tenantId;
    private final PolicyType policyType;

    PolicyInfo(String tenantId, PolicyType policyType) {
      this.tenantId = tenantId;
      this.policyType = policyType;
    }

    public String getTenantId() {
      return tenantId;
    }

    public PolicyType getPolicyType() {
      return policyType;
    }

    @Override
    public String toString() {
      return "PolicyInfo{tenantId='" + tenantId + '\'' + ", policyType=" + policyType + '}';
    }
  }
}
