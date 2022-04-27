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

import static java.lang.Thread.sleep;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTITENANCY_RANGER_SYNC_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTITENANCY_RANGER_SYNC_INTERVAL_DEFAULT;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Background Sync thread that reads Multi-Tenancy state from OM DB
 * and applies it to Ranger.
 */
public class OMRangerBGSyncService implements Runnable, Closeable {

  private OzoneManager ozoneManager;
  private OMMetadataManager metadataManager;
  private OMMultiTenantManager multiTenantManager;
  private OzoneClient ozoneClient;

  private static final Logger LOG = LoggerFactory
      .getLogger(OMRangerBGSyncService.class);
  private static final int WAIT_MILI = 1000;
  private static final int MAX_ATTEMPT = 2;

  /**
   * ExecutorService used for scheduling sync operation.
   */
  private final ScheduledExecutorService executorService;
  private ScheduledFuture<?> rangerSyncFuture;
  private final long rangerSyncInterval;

  private long rangerBGSyncCounter = 0;
  private long currentOzoneServiceVersionInOMDB;
  private long proposedOzoneServiceVersionInOMDB;
  private final int rangerOzoneServiceId;

  private MultiTenantAccessAuthorizerRangerPlugin authorizer;

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

  // we will track all the policies in Ranger here. After we have
  // processed all the policies from OMDB, this map will
  // be left with policies that we need to delete.
  //
  // Maps from policy name to policy ID in Ranger
  private HashMap<String, String> mtRangerPoliciesTobeDeleted = new HashMap<>();

  // This map will be used to keep all the policies that are found in
  // OMDB and should have been in Ranger. Currently, we are only printing such
  // policyID. This can result if a tenant is deleted but the system
  // crashed. Its an easy recovery to retry the "tenant delete" operation.
  //
  // Maps from policy name to policy ID in Ranger
  private HashMap<String, String> mtRangerPoliciesTobeCreated = new HashMap<>();

  // This map will keep all the Multiotenancy related roles from Ranger.
  private HashMap<String, BGRole> mtRangerRoles = new HashMap<>();

  // keep OMDB mapping of Roles -> list of user principals.
  private HashMap<String, HashSet<String>> mtOMDBRoles = new HashMap<>();

  // Every BG ranger sync cycle we update this
  private long lastRangerPolicyLoadTime;

  public OMRangerBGSyncService(OzoneManager ozoneManager) throws IOException {
    try {
      this.ozoneManager = ozoneManager;
      metadataManager = ozoneManager.getMetadataManager();
      authorizer = new MultiTenantAccessAuthorizerRangerPlugin();
      authorizer.init(ozoneManager.getConfiguration());
      ozoneClient =
          OzoneClientFactory.getRpcClient(this.ozoneManager.getConfiguration());
      executorService = HadoopExecutors.newScheduledThreadPool(1,
          new ThreadFactoryBuilder().setDaemon(true)
              .setNameFormat("OM Ranger Sync Thread - %d").build());
      rangerSyncInterval = this.ozoneManager.getConfiguration().getTimeDuration(
          OZONE_OM_MULTITENANCY_RANGER_SYNC_INTERVAL,
          OZONE_OM_MULTITENANCY_RANGER_SYNC_INTERVAL_DEFAULT.getDuration(),
          OZONE_OM_MULTITENANCY_RANGER_SYNC_INTERVAL_DEFAULT.getUnit());
      rangerOzoneServiceId = authorizer.getRangerOzoneServiceId();
    } catch (IOException ex) {
      LOG.warn("Failed to Initialize Ranger Background Sync Service: {}",
          ex.getMessage());
      throw ex;
    }
  }

  public void start() {
    isServiceStarted = true;
    scheduleNextRangerSync();
  }

  public void stop() {
    isServiceStarted = false;
  }

  public int getRangerOzoneServiceId() {
    return rangerOzoneServiceId;
  }

  public long getRangerSyncInterval() {
    return rangerSyncInterval;
  }

  private boolean shouldRun() {
    if (ozoneManager == null) {
      // OzoneManager can be null for testing
      return true;
    }
    // The service only runs if current OM node is leader and is ready
    //  and the service is marked as started
    return isServiceStarted && ozoneManager.isLeaderReady();
  }

  @Override
  public void run() {
    // Check OM leader and readiness
    if (shouldRun()) {
      try {
        executeOneRangerSyncCycle();
      } catch (Exception e) {
        LOG.warn("Exception during OM Ranger Sync: {}", e.getMessage());
      } finally {
        // Lets Schedule the next cycle now. We do not deliberaty schedule at
        // fixed interval to account for ranger sync processing time.
        scheduleNextRangerSync();
      }
    } else {
      scheduleNextRangerSync();
    }
  }

  private void scheduleNextRangerSync() {

    if (!Thread.currentThread().isInterrupted() &&
        !executorService.isShutdown()) {
      rangerSyncFuture = executorService.schedule(this,
          rangerSyncInterval, TimeUnit.SECONDS);
    } else {
      LOG.warn("Current Thread is interrupted, shutting down Ranger Sync " +
          "processing thread for Ozone MultiTenant Manager.");
    }
  }

  @Override
  public void close() {
    stop();
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }

      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        LOG.error("Unable to shutdown OM Ranger Background Sync properly.");
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  private void executeOneRangerSyncCycle() {
    int attempt = 0;
    try {
      // Taking the lock only makes sure that while we are reading
      // the current Ozone service version, another multitenancy
      // request is not changing it. We can drop the lock after that.
      while (!multiTenantManager.tryAcquireInProgressMtOp(WAIT_MILI)) {
        sleep(10);
      }
      currentOzoneServiceVersionInOMDB = getOmdbRangerServiceVersion();
      proposedOzoneServiceVersionInOMDB = getRangerServiceVersion();
      while (currentOzoneServiceVersionInOMDB !=
          proposedOzoneServiceVersionInOMDB) {
        if (++attempt > MAX_ATTEMPT) {
          break;
        }
        multiTenantManager.resetInProgressMtOpState();
        if (!ozoneManager.isLeaderReady()) {
          return;
        }
        LOG.warn("Executing Ranger Sync Cycle.");

        executeOmdbToRangerSync(currentOzoneServiceVersionInOMDB);

        if (currentOzoneServiceVersionInOMDB !=
            proposedOzoneServiceVersionInOMDB) {
          // Submit Ratis Request to sync the new ozone service version in OMDB
          setOmdbRangerServiceVersion(proposedOzoneServiceVersionInOMDB);
        }
        while (!multiTenantManager.tryAcquireInProgressMtOp(WAIT_MILI)) {
          sleep(10);
        }
        currentOzoneServiceVersionInOMDB = proposedOzoneServiceVersionInOMDB;
        proposedOzoneServiceVersionInOMDB = getRangerServiceVersion();
      }
    } catch (Exception e) {
      LOG.warn("Exception during a Ranger Sync Cycle: {}", e.getMessage());
    } finally {
      multiTenantManager.resetInProgressMtOpState();
    }
  }

  public long getRangerServiceVersion() throws IOException {
    return authorizer.getCurrentOzoneServiceVersion(rangerOzoneServiceId);
  }

  public void setOmdbRangerServiceVersion(long version) throws IOException {
    // OMDB update goes through RATIS
    ozoneClient.getObjectStore().rangerServiceVersionSync(version);
  }

  public long getOmdbRangerServiceVersion() {
    long lastKnownVersion = 0;
    try {
      lastKnownVersion =
          ozoneManager.getMetadataManager().getOmRangerStateTable()
              .get(OmMetadataManagerImpl.RANGER_OZONE_SERVICE_VERSION_KEY);
    } catch (Exception ex) {
      return 0;
    }
    return lastKnownVersion;
  }

  private void executeOmdbToRangerSync(long baseVersion) throws IOException {
    ++rangerBGSyncCounter;
    cleanupSyncState();
    loadAllPoliciesRolesFromRanger(baseVersion);
    loadAllRolesFromRanger();
    loadAllRolesFromOMDB();

    // This should isolate policies into two groups
    // 1. mtRangerPoliciesTobeDeleted and
    // 2. mtRangerPoliciesTobeCreated
    processAllPoliciesFromOMDB();

    // This should isolate roles that need fixing into a list of
    // roles that need to be replayed back into ranger to get in sync with OMDB.
    processAllRolesFromOMDB();
  }

  public void cleanupSyncState() {
    mtRangerRoles.clear();
    mtRangerPoliciesTobeDeleted.clear();
    mtRangerPoliciesTobeCreated.clear();
    mtOMDBRoles.clear();
  }

  public void loadAllPoliciesRolesFromRanger(long baseVersion)
      throws IOException {
    // TODO: incremental policies API is broken. We are getting all the
    //  multitenant policies using Ranger labels.
    String allPolicies = authorizer.getAllMultiTenantPolicies(
        rangerOzoneServiceId);
    JsonObject jObject = new JsonParser().parse(allPolicies).getAsJsonObject();
    lastRangerPolicyLoadTime = jObject.get("queryTimeMS").getAsLong();
    JsonArray policyArray = jObject.getAsJsonArray("policies");
    for (int i = 0; i < policyArray.size(); ++i) {
      JsonObject newPolicy = policyArray.get(i).getAsJsonObject();
      if (!newPolicy.getAsJsonArray("policyLabels").get(0)
          .getAsString().equals("OzoneMultiTenant")) {
        LOG.warn("Apache Ranger BG Sync: received non MultiTenant policy");
        continue;
      }
      mtRangerPoliciesTobeDeleted.put(
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

  public void loadAllRolesFromRanger() throws IOException {
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
    if (mtRangerPoliciesTobeDeleted.containsKey(policyName)) {
      // This entry is in sync with ranger, remove it from the set
      // Eventually mtRangerPolicies will only contain entries that
      // are not in OMDB and should be removed from Ranger.
      mtRangerPoliciesTobeDeleted.remove(policyName);
    } else {
      // We could not find a policy in ranger that should have been there.
      mtRangerPoliciesTobeCreated.put(policyName, null);
    }
  }

  public void processAllPoliciesFromOMDB() throws IOException {

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

    for (Map.Entry<String, String> entry:
        mtRangerPoliciesTobeCreated.entrySet()) {
      final String policyName = entry.getKey();
      // TODO: Currently we are not maintaining enough information in OMDB
      //  to recreate the policies as-is.
      //  Maybe recreate the policy with its initial value?
      LOG.warn("Policy name not found in Ranger: '{}'. "
          + "OM can't fix this automatically", policyName);
    }

    for (Map.Entry<String, String> entry:
        mtRangerPoliciesTobeDeleted.entrySet()) {
      // TODO: Its best to not create these policies automatically and the
      //  let the user delete the tenant and recreate the tenant.
      String policyName = entry.getKey();
      AccessPolicy accessPolicy = authorizer.getAccessPolicyByName(policyName);
      if (lastRangerPolicyLoadTime >
          (accessPolicy.getLastUpdateTime() + 3600 * 1000)) {
        LOG.warn("Deleting policies from Ranger: " + policyName);
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

  public void loadAllRolesFromOMDB() throws IOException {
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

  public void processAllRolesFromOMDB() throws IOException {
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

  public void pushOMDBRoleToRanger(String roleName) throws IOException {
    HashSet<String> omdbUserList = mtOMDBRoles.get(roleName);
    String roleJsonStr = authorizer.getRole(roleName);
    authorizer.assignAllUsers(omdbUserList, roleJsonStr);
  }

  public void setOzoneClient(OzoneClient oc) {
    this.ozoneClient = oc;
  }

  public long getRangerBGSyncCounter() {
    return rangerBGSyncCounter;
  }
}
