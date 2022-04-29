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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult.EmptyTaskResult;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.OMMultiTenantManagerImpl;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.om.multitenant.CachedTenantState.CachedAccessIdInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RangerServiceVersionSyncRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Background Sync thread that reads Multi-Tenancy state from OM DB
 * and applies it to Ranger.
 */
public class OMRangerBGSyncService extends BackgroundService {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMRangerBGSyncService.class);
  private static final ClientId CLIENT_ID = ClientId.randomId();
  private static final long ONE_HOUR_IN_MILLIS = 3600 * 1000;

  private final OzoneManager ozoneManager;
  private final OMMetadataManager metadataManager;
  private final OMMultiTenantManager multiTenantManager;
  private final MultiTenantAccessAuthorizer authorizer;

  // Maximum number of attempts for each sync run
  private static final int MAX_ATTEMPT = 2;
  private final AtomicLong runCount = new AtomicLong(0);
  private int rangerOzoneServiceId;

  private boolean isServiceStarted = false;

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
      return name.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      // TODO: Do we care about userSet
      return this.hashCode() == o.hashCode();
    }
  }

  // This map will be used to keep all the policies that are found in
  // OM DB and should have been in Ranger. Currently, we are only printing such
  // policyID. This can result if a tenant is deleted but the system
  // crashed. Its an easy recovery to retry the "tenant delete" operation.
  //
  // Maps from policy name to policy ID in Ranger
  private final HashMap<String, String> mtRangerPoliciesToBeCreated =
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

  // Every BG ranger sync cycle we update this
  private long lastRangerPolicyLoadTime;

  public OMRangerBGSyncService(OzoneManager ozoneManager,
      MultiTenantAccessAuthorizer authorizer, long interval,
      TimeUnit unit, long serviceTimeout)
      throws IOException {
    super("OMRangerBGSyncService", interval, unit, 1, serviceTimeout);

    this.ozoneManager = ozoneManager;
    this.metadataManager = ozoneManager.getMetadataManager();
    this.multiTenantManager = ozoneManager.getMultiTenantManager();

    this.authorizer = authorizer;

    if (authorizer != null) {
      if (authorizer instanceof MultiTenantAccessAuthorizerRangerPlugin) {
        MultiTenantAccessAuthorizerRangerPlugin rangerAuthorizer =
            (MultiTenantAccessAuthorizerRangerPlugin) authorizer;
        rangerOzoneServiceId = rangerAuthorizer.getRangerOzoneServiceId();
      } else if (
          !(authorizer instanceof MultiTenantAccessAuthorizerDummyPlugin)) {
        throw new OMException("Unsupported MultiTenantAccessAuthorizer: " +
            authorizer.getClass().getSimpleName(),
            OMException.ResultCodes.INTERNAL_ERROR);
      }
    } else {
      // authorizer can be null for unit tests
      LOG.warn("MultiTenantAccessAuthorizer is not set");
    }
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new RangerBGSyncTask());
    return queue;
  }

  public void start() {
    if (authorizer == null) {
      LOG.error("Failed to start the background sync service: "
          + "null authorizer. Please check OM configuration. Aborting");
      return;
    }
    isServiceStarted = true;
    super.start();
  }

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

      long currentOzoneServiceVerInDB = getOMDBRangerServiceVersion();
      long proposedOzoneServiceVerInDB = retrieveRangerServiceVersion();
      while (currentOzoneServiceVerInDB != proposedOzoneServiceVerInDB) {

        if (++attempt > MAX_ATTEMPT) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Reached maximum number of attempts ({}). Abort",
                MAX_ATTEMPT);
          }
          break;
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("Ranger Ozone service version ({}) differs from DB's ({}). "
                  + "Starting to sync.",
              proposedOzoneServiceVerInDB, currentOzoneServiceVerInDB);
        }

        LOG.info("Executing Multi-Tenancy Ranger Sync: run #{}, attempt #{}",
            runCount.get(), attempt);

        executeOMDBToRangerSync(currentOzoneServiceVerInDB);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Setting OM DB Ranger Service Version to {} (was {})",
              proposedOzoneServiceVerInDB, currentOzoneServiceVerInDB);
        }
        // Submit Ratis Request to sync the new service version in OM DB
        setOMDBRangerServiceVersion(proposedOzoneServiceVerInDB);

        // Check Ranger ozone service version again
        currentOzoneServiceVerInDB = proposedOzoneServiceVerInDB;
        proposedOzoneServiceVerInDB = retrieveRangerServiceVersion();
      }
    } catch (IOException e) {
      LOG.warn("Exception during Ranger Sync", e);
      // TODO: Check specific exception once switched to
      //  RangerRestMultiTenantAccessController
    }

  }

  long retrieveRangerServiceVersion() throws IOException {
    return authorizer.getCurrentOzoneServiceVersion(rangerOzoneServiceId);
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

  void setOMDBRangerServiceVersion(long version) {
    // OM DB update goes through Ratis
    RangerServiceVersionSyncRequest.Builder versionSyncRequest =
        RangerServiceVersionSyncRequest.newBuilder()
            .setRangerServiceVersion(version);

    OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(Type.RangerServiceVersionSync)
        .setRangerServiceVersionSyncRequest(versionSyncRequest)
        .setClientId(CLIENT_ID.toString())
        .build();

    // Submit PurgeKeys request to OM
    try {
      RaftClientRequest raftClientRequest = newRaftClientRequest(omRequest);
      ozoneManager.getOmRatisServer().submitRequest(omRequest,
          raftClientRequest);
    } catch (ServiceException e) {
      LOG.error("RangerServiceVersionSync request failed. "
          + "Will retry at next run.");
    }
  }

  long getOMDBRangerServiceVersion() throws IOException {
    return ozoneManager.getMetadataManager().getOmRangerStateTable()
        .get(OmMetadataManagerImpl.RANGER_OZONE_SERVICE_VERSION_KEY);
  }

  private void executeOMDBToRangerSync(long baseVersion) throws IOException {
    clearPolicyAndRoleMaps();

    loadAllPoliciesRolesFromRanger(baseVersion);
    loadAllRolesFromRanger();
    loadAllRolesFromOM();

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
  private void loadAllPoliciesRolesFromRanger(long baseVersion)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("baseVersion is {}", baseVersion);
    }
    // TODO: incremental policies API is broken. We are getting all the
    //  Multi-Tenant policies using Ranger labels.
    String allPolicies = authorizer.getAllMultiTenantPolicies(
        rangerOzoneServiceId);
    JsonObject jObject = new JsonParser().parse(allPolicies).getAsJsonObject();
    lastRangerPolicyLoadTime = jObject.get("queryTimeMS").getAsLong();
    JsonArray policyArray = jObject.getAsJsonArray("policies");
    for (int i = 0; i < policyArray.size(); ++i) {
      JsonObject newPolicy = policyArray.get(i).getAsJsonObject();
      if (!newPolicy.getAsJsonArray("policyLabels").get(0)
          .getAsString().equals("OzoneMultiTenant")) {
        LOG.warn("Apache Ranger BG Sync: received non Multi-Tenant policy: {}",
            newPolicy.get("name").getAsString());  // TODO: Reduce to debug?
        continue;
      }
      mtRangerPoliciesToBeDeleted.put(
          newPolicy.get("name").getAsString(),
          newPolicy.get("id").getAsString());
      JsonArray policyItems = newPolicy
          .getAsJsonArray("policyItems");
      for (int j = 0; j < policyItems.size(); ++j) {
        JsonObject policy = policyItems.get(j).getAsJsonObject();
        JsonArray roles = policy.getAsJsonArray("roles");
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

  private void loadAllRolesFromRanger() throws IOException {
    for (Map.Entry<String, BGRole> entry: mtRangerRoles.entrySet()) {
      final String roleName = entry.getKey();
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
  private void mtRangerPoliciesOpHelper(String policyName) {
    if (mtRangerPoliciesToBeDeleted.containsKey(policyName)) {
      // This entry is in sync with ranger, remove it from the set
      // Eventually mtRangerPolicies will only contain entries that
      // are not in OMDB and should be removed from Ranger.
      mtRangerPoliciesToBeDeleted.remove(policyName);
    } else {
      // We could not find a policy in ranger that should have been there.
      mtRangerPoliciesToBeCreated.put(policyName, null);
    }
  }

  private void processAllPoliciesFromOMDB() throws IOException {

    // Iterate all DB tenant states. For each tenant,
    // queue or dequeue bucketNamespacePolicyName and bucketPolicyName
    TableIterator<String, ? extends Table.KeyValue<String, OmDBTenantState>>
        tenantStateTableIter = metadataManager.getTenantStateTable().iterator();
    while (tenantStateTableIter.hasNext()) {
      final Table.KeyValue<String, OmDBTenantState> tableKeyValue =
          tenantStateTableIter.next();
      final OmDBTenantState dbTenantState = tableKeyValue.getValue();
      mtRangerPoliciesOpHelper(dbTenantState.getBucketNamespacePolicyName());
      mtRangerPoliciesOpHelper(dbTenantState.getBucketPolicyName());
    }

    for (Map.Entry<String, String> entry :
        mtRangerPoliciesToBeCreated.entrySet()) {
      final String policyName = entry.getKey();
      // TODO: Currently we are not maintaining enough information in OM DB
      //  to recreate the policies as-is.
      //  Maybe recreate the policy with its initial value?
      LOG.warn("Policy name not found in Ranger: '{}'. "
          + "OM can't fix this automatically", policyName);
    }

    for (Map.Entry<String, String> entry :
        mtRangerPoliciesToBeDeleted.entrySet()) {
      // TODO: Its best to not create these policies automatically and the
      //  let the user delete the tenant and recreate the tenant.
      String policyName = entry.getKey();

      // TODO: Use AccessController instead of AccessPolicy
//      MultiTenantAccessController accessController =
//          authorizer.getMultiTenantAccessController();
      AccessPolicy accessPolicy = authorizer.getAccessPolicyByName(policyName);

      if (lastRangerPolicyLoadTime >
          (accessPolicy.getPolicyLastUpdateTime() + ONE_HOUR_IN_MILLIS)) {
        LOG.warn("Deleting policy from Ranger: {}", policyName);
        authorizer.deletePolicybyName(policyName);
        for (String deletedRole : accessPolicy.getRoleList()) {
          authorizer.deleteRole(new JsonParser()
              .parse(authorizer.getRole(deletedRole))
              .getAsJsonObject().get("id").getAsString());
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
      // TODO: Most likely unnecessary. Got reference above. Double check
//      mtOMDBRoles.put(roleName, usersInTheRole);
    }
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
    final Map<String, CachedTenantState> tenantCache = impl.getTenantCache();

    impl.acquireTenantCacheReadLock();

    try {
      // tenantCache: tenantId -> CachedTenantState
      for (Map.Entry<String, CachedTenantState> e1 : tenantCache.entrySet()) {
        final CachedTenantState cachedTenantState = e1.getValue();

        final String userRoleName = cachedTenantState.getTenantUserRoleName();
        final String adminRoleName = cachedTenantState.getTenantAdminRoleName();

        final Map<String, CachedAccessIdInfo> accessIdInfoMap =
            cachedTenantState.getAccessIdInfoMap();

        // accessIdInfoMap: accessId -> CachedAccessIdInfo
        for (Map.Entry<String, CachedAccessIdInfo> e2 :
            accessIdInfoMap.entrySet()) {
          final CachedAccessIdInfo cachedAccessIdInfo = e2.getValue();

          final String userPrincipal = cachedAccessIdInfo.getUserPrincipal();
          final boolean isAdmin = cachedAccessIdInfo.getIsAdmin();

          addUserToMtOMDBRoles(userRoleName, userPrincipal);

          if (isAdmin) {
            addUserToMtOMDBRoles(adminRoleName, userPrincipal);
          }
        }
      }
    } finally {
      impl.releaseTenantCacheReadLock();
    }

  }

  private void loadAllRolesFromDB() throws IOException {
    // We have the following in OM DB
    //  tenantStateTable: tenantId -> TenantState (has user and admin role name)
    //  tenantAccessIdTable : accessId -> OmDBAccessIdInfo

    final Table<String, OmDBTenantState> tenantStateTable =
        metadataManager.getTenantStateTable();

    // Iterate all DB ExtendedUserAccessIdInfo. For each accessId,
    // add to userRole. And add to adminRole if isAdmin is set.
    TableIterator<String, ? extends Table.KeyValue<String, OmDBAccessIdInfo>>
        tenantAccessIdTableIter =
        metadataManager.getTenantAccessIdTable().iterator();
    while (tenantAccessIdTableIter.hasNext()) {
      final Table.KeyValue<String, OmDBAccessIdInfo> tableKeyValue =
          tenantAccessIdTableIter.next();
      final OmDBAccessIdInfo dbAccessIdInfo = tableKeyValue.getValue();

      final String tenantId = dbAccessIdInfo.getTenantId();
      final String userPrincipal = dbAccessIdInfo.getUserPrincipal();

      final OmDBTenantState dbTenantState = tenantStateTable.get(tenantId);
      final String userRole = dbTenantState.getUserRoleName();

      // Every tenant user should be in the tenant's userRole
      addUserToMtOMDBRoles(userRole, userPrincipal);

      // If the accessId has admin flag set, add to adminRole as well
      if (dbAccessIdInfo.getIsAdmin()) {
        final String adminRole = dbTenantState.getAdminRoleName();
        addUserToMtOMDBRoles(adminRole, userPrincipal);
      }
    }
  }

  private void processAllRolesFromOMDB() throws IOException {
    // Lets First make sure that all the Roles in OMDB are present in Ranger
    // as well as the corresponding userlist matches matches.
    for (Map.Entry<String, HashSet<String>> entry: mtOMDBRoles.entrySet()) {
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
            // We found a user in OMDB Role that doesn't exist in Ranger Role.
            // Lets just push the role from OMDB to Ranger.
            pushRoleToRanger = true;
            break;
          }
        }
        // We have processed all the Userlist entries in the OMDB. If
        // ranger Userlist is not empty, we are not in sync with ranger.
        if (!rangerUserList.isEmpty()) {
          pushRoleToRanger = true;
        }
      } else {
        // This role is missing from Ranger, we need to push this in Ranger.
        authorizer.createRole(roleName, null);
        pushRoleToRanger = true;
      }
      if (pushRoleToRanger) {
        pushOMDBRoleToRanger(roleName);
      }
      // We have processed this role in OMDB now. Lets remove it from
      // mtRangerRoles. Eventually whatever is left in mtRangerRoles
      // are extra entries, that should not have been in Ranger.
      mtRangerRoles.remove(roleName);
    }

    for (String roleName: mtRangerRoles.keySet()) {
      authorizer.deleteRole(new JsonParser().parse(authorizer.getRole(roleName))
          .getAsJsonObject().get("id").getAsString());
    }
  }

  private void pushOMDBRoleToRanger(String roleName) throws IOException {
    HashSet<String> omdbUserList = mtOMDBRoles.get(roleName);
    String roleJsonStr = authorizer.getRole(roleName);
    authorizer.assignAllUsers(omdbUserList, roleJsonStr);
  }

  public long getRangerSyncRunCount() {
    return runCount.get();
  }
}
