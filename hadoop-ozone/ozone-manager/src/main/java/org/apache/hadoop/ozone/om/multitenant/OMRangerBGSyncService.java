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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.multitenant;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.protobuf.ServiceException;
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
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetRangerServiceVersionRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_TENANT_RANGER_POLICY_LABEL;

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

  public static final Logger LOG =
      LoggerFactory.getLogger(OMRangerBGSyncService.class);
  private static final ClientId CLIENT_ID = ClientId.randomId();

  private final OzoneManager ozoneManager;
  private final OMMetadataManager metadataManager;
  private final OMMultiTenantManager multiTenantManager;
  private final MultiTenantAccessAuthorizer authorizer;

  // Maximum number of attempts for each sync run
  private static final int MAX_ATTEMPT = 2;
  private final AtomicLong runCount = new AtomicLong(0);

  private volatile boolean isServiceStarted = false;

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
      return "PolicyInfo{" +
          "tenantId='" + tenantId + '\'' + ", policyType=" + policyType + '}';
    }
  }

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
      MultiTenantAccessAuthorizer authorizer, long interval,
      TimeUnit unit, long serviceTimeout) {

    super("OMRangerBGSyncService", interval, unit, 1, serviceTimeout);

    this.ozoneManager = ozoneManager;
    this.metadataManager = ozoneManager.getMetadataManager();
    this.multiTenantManager = ozoneManager.getMultiTenantManager();

    if (authorizer != null) {
      this.authorizer = authorizer;
    } else {
      // authorizer can be null for unit tests
      LOG.warn("MultiTenantAccessAuthorizer not set. Using dummy authorizer");
      this.authorizer = new MultiTenantAccessAuthorizerDummyPlugin();
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
    if (authorizer == null) {
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
        runCount.incrementAndGet();
        triggerRangerSyncOnce();
      }

      return EmptyTaskResult.newResult();
    }
  }

  private void triggerRangerSyncOnce() {
    int attempt = 0;
    try {
      // TODO: Acquire lock
      long dbOzoneServiceVersion = getOMDBRangerServiceVersion();
      long rangerOzoneServiceVersion = getLatestRangerServiceVersion();

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
        // TODO: Release lock
        if (++attempt > MAX_ATTEMPT) {
          if (LOG.isDebugEnabled()) {
            LOG.warn("Reached maximum number of attempts ({}). Abort",
                MAX_ATTEMPT);
          }
          break;
        }

        LOG.info("Executing Multi-Tenancy Ranger Sync: run #{}, attempt #{}. "
                + "Ranger service version: {}, DB version :{}",
            runCount.get(), attempt,
            rangerOzoneServiceVersion, dbOzoneServiceVersion);

        executeOMDBToRangerSync(dbOzoneServiceVersion);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Setting OM DB Ranger Service Version to {} (was {})",
              rangerOzoneServiceVersion, dbOzoneServiceVersion);
        }
        // Submit Ratis Request to sync the new service version in OM DB
        setOMDBRangerServiceVersion(rangerOzoneServiceVersion);

        // TODO: Acquire lock

        // Check Ranger ozone service version again
        dbOzoneServiceVersion = rangerOzoneServiceVersion;
        rangerOzoneServiceVersion = getLatestRangerServiceVersion();
      }
    } catch (IOException | ServiceException e) {
      LOG.warn("Exception during Ranger Sync", e);
      // TODO: Check specific exception once switched to
      //  RangerRestMultiTenantAccessController
//    } finally {
//      // TODO: Release lock
    }

  }

  /**
   * Query Ranger endpoint to get the latest Ozone service version.
   */
  long getLatestRangerServiceVersion() throws IOException {
    return authorizer.getLatestOzoneServiceVersion();
  }

  private RaftClientRequest newRaftClientRequest(OMRequest omRequest) {
    return RaftClientRequest.newBuilder()
        .setClientId(CLIENT_ID)
        .setServerId(ozoneManager.getOmRatisServer().getRaftPeerId())
        .setGroupId(ozoneManager.getOmRatisServer().getRaftGroupId())
        .setCallId(runCount.get())
        .setMessage(
            Message.valueOf(
                OMRatisHelper.convertRequestToByteString(omRequest)))
        .setType(RaftClientRequest.writeRequestType())
        .build();
  }

  void setOMDBRangerServiceVersion(long version) throws ServiceException {
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
      RaftClientRequest raftClientRequest = newRaftClientRequest(omRequest);
      ozoneManager.getOmRatisServer().submitRequest(omRequest,
          raftClientRequest);
    } catch (ServiceException e) {
      LOG.error("SetRangerServiceVersion request failed. "
          + "Will retry at next run.");
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

    // TODO: Acquire global lock
    loadAllPoliciesAndRoleNamesFromRanger(baseVersion);
    loadAllRolesFromRanger();
    loadAllRolesFromOM();
    // TODO: Release global lock

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

  /**
   * TODO: Test and make sure invalid JSON response from Ranger won't crash OM.
   */
  private void loadAllPoliciesAndRoleNamesFromRanger(long baseVersion)
      throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("baseVersion is {}", baseVersion);
    }

    String allPolicies = authorizer.getAllMultiTenantPolicies();
    JsonObject jObject = new JsonParser().parse(allPolicies).getAsJsonObject();
    JsonArray policies = jObject.getAsJsonArray("policies");
    for (int i = 0; i < policies.size(); ++i) {
      JsonObject policy = policies.get(i).getAsJsonObject();
      JsonArray policyLabels = policy.getAsJsonArray("policyLabels");

      // Verify that the policy has the OzoneTenant label
      boolean hasOzoneTenantLabel = false;
      // Loop just in case multiple labels are attached to the tenant policy
      for (int j = 0; j < policyLabels.size(); j++) {
        final String currentLabel = policyLabels.get(j).getAsString();
        // Look for exact match
        if (currentLabel.equals(OZONE_TENANT_RANGER_POLICY_LABEL)) {
          hasOzoneTenantLabel = true;
          break;
        }
      }

      if (!hasOzoneTenantLabel) {
        // Shouldn't get policies without the label often as it is
        // specified in the query param, unless a user removed the tag during
        // the sync
        LOG.warn("Ignoring Ranger policy without the {} label: {}",
            OZONE_TENANT_RANGER_POLICY_LABEL, policy.get("name").getAsString());
        continue;
      }

      // Temporarily put the policy in the to-delete list,
      // valid entries will be removed later
      mtRangerPoliciesToBeDeleted.put(
          policy.get("name").getAsString(),
          policy.get("id").getAsString());

      final JsonArray policyItems = policy.getAsJsonArray("policyItems");
      for (int j = 0; j < policyItems.size(); ++j) {
        JsonObject policyItem = policyItems.get(j).getAsJsonObject();
        JsonArray roles = policyItem.getAsJsonArray("roles");
        for (int k = 0; k < roles.size(); ++k) {
          if (!mtRangerRoles.containsKey(roles.get(k).getAsString())) {
            // We only get the role name here. We need to query and populate it.
            mtRangerRoles.put(roles.get(k).getAsString(),
                new BGRole(roles.get(k).getAsString()));
          }
        }
      }
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
    if (!ozoneManager.isLeaderReady()) {
      throw new OMNotLeaderException("This OM is no longer the leader. Abort");
    }
  }

  private void loadAllRolesFromRanger() throws IOException {
    for (Map.Entry<String, BGRole> entry: mtRangerRoles.entrySet()) {
      final String roleName = entry.getKey();
      checkLeader();
      final String roleDataString = authorizer.getRole(roleName);
      final JsonObject roleObject =
          new JsonParser().parse(roleDataString).getAsJsonObject();
      final BGRole role = entry.getValue();
      role.setId(roleObject.get("id").getAsString());
      final JsonArray userArray = roleObject.getAsJsonArray("users");
      for (int i = 0; i < userArray.size(); ++i) {
        role.addUserPrincipal(userArray.get(i).getAsJsonObject().get("name")
            .getAsString());
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
        Preconditions.checkNotNull(volumeName);

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
      authorizer.deletePolicyByName(policyName);
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

    final AccessPolicy accessPolicy;

    switch (policyInfo.getPolicyType()) {
    case BUCKET_NAMESPACE_POLICY:
      LOG.info("Recovering VolumeAccess policy for tenant: {}", tenantId);

      final String adminRoleName =
          multiTenantManager.getTenantAdminRoleName(tenantId);

      accessPolicy = multiTenantManager.newDefaultVolumeAccessPolicy(volumeName,
          new OzoneTenantRolePrincipal(userRoleName),
          new OzoneTenantRolePrincipal(adminRoleName));
      break;

    case BUCKET_POLICY:
      LOG.info("Recovering BucketAccess policy for tenant: {}", tenantId);

      accessPolicy = multiTenantManager.newDefaultBucketAccessPolicy(volumeName,
              new OzoneTenantRolePrincipal(userRoleName));
      break;

    default:
      throw new OMException("Unknown policy type in " + policyInfo,
          ResultCodes.INTERNAL_ERROR);
    }

    String id = authorizer.createAccessPolicy(accessPolicy);
    LOG.info("Created policy, ID: {}", id);
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
        try {
          authorizer.createRole(roleName, null);
        } catch (IOException e) {
          // Tolerate create role failure, possibly due to role already exists
          LOG.error(e.getMessage());
        }
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
      try {
        final String roleObj = authorizer.getRole(roleName);
        authorizer.deleteRole(new JsonParser().parse(roleObj)
            .getAsJsonObject().get("id").getAsString());
      } catch (IOException e) {
        // The role might have been deleted already.
        // Or the role could be referenced in other roles or policies.
        LOG.error("Failed to delete role: {}", roleName);
        throw e;
      }
      // TODO: Server returned HTTP response code: 400
      //  if already deleted or is depended on
    }
  }

  private void pushOMDBRoleToRanger(String roleName) throws IOException {
    HashSet<String> omdbUserList = mtOMDBRoles.get(roleName);
    String roleJsonStr = authorizer.getRole(roleName);
    authorizer.assignAllUsers(omdbUserList, roleJsonStr);
  }

  /**
   * Return the number of runs the sync is triggered.
   *
   * This doesn't count attempts inside each sync run.
   */
  public long getRangerSyncRunCount() {
    return runCount.get();
  }
}
