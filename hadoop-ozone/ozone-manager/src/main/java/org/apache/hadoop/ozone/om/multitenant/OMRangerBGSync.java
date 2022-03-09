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
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_SYNC_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_SYNC_INTERVAL_DEFAULT;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Background Sync thread that reads Multitnancy state from OMDB
 * and applies it to Ranger.
 */
public class OMRangerBGSync implements Runnable, Closeable {

  private OzoneManager ozoneManager;
  private OzoneClient ozoneClient;

  private static final Logger LOG = LoggerFactory
      .getLogger(OMRangerBGSync.class);
  private static final int WAIT_MILI = 1000;
  private static final int MAX_ATTEMPT = 2;

  /**
   * ExecutorService used for scheduling sync operation.
   */
  private final ScheduledExecutorService executorService;
  private ScheduledFuture<?> rangerSyncFuture;
  private final int rangerSyncInterval;

  private long rangerBGSyncCounter = 0;
  private long currentOzoneServiceVersionInOMDB;
  private long proposedOzoneServiceVersionInOMDB;
  private static int ozoneServiceId;

  private MultiTenantAccessAuthorizerRangerPlugin authorizer;

  class BGRole {
    private String name;
    private String id;
    private HashSet<String> users;

    BGRole(String n) {
      this.name = n;
      users = new HashSet<>();
    }

    public void addId(String i) {
      this.id = i;
    }

    public void addUsers(String u) {
      users.add(u);
    }

    public HashSet<String> getUsers() {
      return users;
    }

    @Override
    public int hashCode() {
      return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return (this.hashCode() == obj.hashCode());
    }
  }

  // we will track all the policies in Ranger here. After we have
  // processed all the policies from OMDB, this map will
  // be left with policies that we need to delete.
  // Its a map of Policy ID to policy names
  private ConcurrentHashMap<String, String> mtRangerPoliciesTobeDeleted =
      new ConcurrentHashMap<>();

  // This map will be used to keep all the policies that are found in
  // OMDB and should have been in Ranger. Currently, we are only printing such
  // policyID. This can result if a tenant is deleted but the system
  // crashed. Its an easy recovery to retry the "tenant delete" operation.
  // Its a map of policy ID to policy names
  private ConcurrentHashMap<String, String> mtRangerPoliciesTobeCreated =
      new ConcurrentHashMap<>();

  // This map will keep all the Multiotenancy related roles from Ranger.
  private ConcurrentHashMap<String, BGRole> mtRangerRoles =
      new ConcurrentHashMap<>();

  // keep OMDB mapping of Roles -> list of user principals.
  private ConcurrentHashMap<String, HashSet<String>> mtOMDBRoles =
      new ConcurrentHashMap<>();

  // Every BG ranger sync cycle we update this
  private long lastRangerPolicyLoadTime;

  public OMRangerBGSync(OzoneManager om) throws Exception {
    try {
      ozoneManager = om;
      authorizer = new MultiTenantAccessAuthorizerRangerPlugin();
      authorizer.init(om.getConfiguration());
      ozoneClient =
          OzoneClientFactory.getRpcClient(ozoneManager.getConfiguration());
      executorService = HadoopExecutors.newScheduledThreadPool(1,
          new ThreadFactoryBuilder().setDaemon(true)
              .setNameFormat("OM Ranger Sync Thread - %d").build());
      rangerSyncInterval =
          ozoneManager.getConfiguration().getInt(OZONE_OM_RANGER_SYNC_INTERVAL,
              OZONE_OM_RANGER_SYNC_INTERVAL_DEFAULT);
      scheduleNextRangerSync();
      ozoneServiceId = authorizer.getOzoneServiceId();
    } catch (Exception e) {
      LOG.warn("Failed to Initialize Ranger Background Sync");
      throw e;
    }
  }

  public int getOzoneServiceId() throws Exception {
    return ozoneServiceId;
  }

  public int getRangerSyncInterval() throws Exception {
    return rangerSyncInterval;
  }

  @Override
  public void run() {
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    try {
      if (ozoneManager.isLeaderReady()) {
        executeOneRangerSyncCycle();
      }
    } catch (Exception e) {
      LOG.warn("Exception during OM Ranger Background Sync." + e.getMessage());
    } finally {
      // Lets Schedule the next cycle now. We do not deliberaty schedule at
      // fixed interval to account for ranger sync processing time.
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
      while (!ozoneManager.getMultiTenantManager()
          .tryAcquireInProgressMtOp(WAIT_MILI)) {
        sleep(10);
      }
      currentOzoneServiceVersionInOMDB = getOmdbRangerServiceVersion();
      proposedOzoneServiceVersionInOMDB = getRangerServiceVersion();
      while (currentOzoneServiceVersionInOMDB !=
          proposedOzoneServiceVersionInOMDB) {
        if (++attempt > MAX_ATTEMPT) {
          break;
        }
        ozoneManager.getMultiTenantManager().resetInProgressMtOpState();
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
        while (!ozoneManager.getMultiTenantManager()
            .tryAcquireInProgressMtOp(WAIT_MILI)) {
          sleep(10);
        }
        currentOzoneServiceVersionInOMDB = proposedOzoneServiceVersionInOMDB;
        proposedOzoneServiceVersionInOMDB = getRangerServiceVersion();
      }
    } catch (Exception e) {
      LOG.warn("Exception during a Ranger Sync Cycle. " + e.getMessage());
    } finally {
      ozoneManager.getMultiTenantManager().resetInProgressMtOpState();
    }
  }

  public long getRangerServiceVersion() throws Exception {
    return authorizer.getCurrentOzoneServiceVersion(ozoneServiceId);
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

  private void executeOmdbToRangerSync(long baseVersion) throws Exception {
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
    //
    processAllRolesFromOMDB();
  }

  public void cleanupSyncState() {
    mtRangerRoles.clear();
    mtRangerPoliciesTobeDeleted.clear();
    mtRangerPoliciesTobeCreated.clear();
    mtOMDBRoles.clear();
  }

  public void loadAllPoliciesRolesFromRanger(long baseVersion)
      throws Exception {
    // TODO: incremental policies API is broken. We are getting all the
    //  multitenant policies using Ranger labels.
    String allPolicies = authorizer.getAllMultiTenantPolicies(ozoneServiceId);
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
      mtRangerPoliciesTobeDeleted.put(newPolicy.get("id").getAsString(),
          newPolicy.get("name").getAsString());
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

  public void loadAllRolesFromRanger() throws Exception {
    for (String rolename: mtRangerRoles.keySet()) {
      String roleDataString = authorizer.getRole(rolename);
      JsonObject roleObject =
          new JsonParser().parse(roleDataString).getAsJsonObject();
      BGRole role = mtRangerRoles.get(rolename);
      role.addId(roleObject.get("id").getAsString());
      JsonArray userArray = roleObject.getAsJsonArray("users");
      for (int i = 0; i < userArray.size(); ++i) {
        role.addUsers(userArray.get(i).getAsJsonObject().get("name")
            .getAsString());
      }
    }
  }

  public void processAllPoliciesFromOMDB() throws Exception {
    TableIterator<String, ? extends Table.KeyValue<String, String>>
        policyTableIterator =
        ozoneManager.getMetadataManager().getTenantPolicyTable().iterator();
    while (policyTableIterator.hasNext()) {
      Table.KeyValue<String, String> keyValue = policyTableIterator.next();
      // Value in the table is a list of policy Ids separated by ",". Please
      // also see OMTenantCreateRequest.java
      String[] omDBPolicies = keyValue.getValue().split(",");

      for (String omDBPolicy: omDBPolicies) {
        if (mtRangerPoliciesTobeDeleted.containsKey(omDBPolicy)) {
          // This entry is in sync with ranger, remove it from the set
          // Eventually mtRangerPolicies will only contain entries that
          // are not in OMDB and should be removed from Ranger.
          mtRangerPoliciesTobeDeleted.remove(omDBPolicy);
        } else {
          // We could not find a policy in ranger that should have been there.
          mtRangerPoliciesTobeCreated.put(omDBPolicy, null);
        }
      }
    }

    for (String policy: mtRangerPoliciesTobeCreated.keySet()) {
      // TODO : Currently we are not maintaining enough information in OMDB
      //  to recreate the policies.
      LOG.warn("Policies not found in Ranger: " + policy);
    }

    for (String policyId: mtRangerPoliciesTobeDeleted.keySet()) {
      // TODO : Its best to not create these poilicies automatically and the
      //  let the user delete the tenant and recreate the tenant.
      String policy = mtRangerPoliciesTobeDeleted.get(policyId);
      AccessPolicy accessPolicy = authorizer.getAccessPolicyByName(policy);
      if (lastRangerPolicyLoadTime >
          (accessPolicy.getLastUpdateTime() + 3600 * 1000)) {
        LOG.warn("Deleting policies from Ranger: " + policy);
        authorizer.deletePolicybyName(policy);
        for (String deletedrole : accessPolicy.getRoleList()) {
          authorizer.deleteRole(new JsonParser()
              .parse(authorizer.getRole(deletedrole))
              .getAsJsonObject().get("id").getAsString());
        }
      }
    }
  }

  public void loadAllRolesFromOMDB() throws Exception {
    // We have following info in OMDB
    //  tenantRoleTable: accessId -> roles [admin, roleB, ...]
    //  tenantAccessIdTable : accessId -> OmDBAccessIdInfo
    TableIterator<String, ? extends Table.KeyValue<String, String>>
        roleTableIterator =
        ozoneManager.getMetadataManager().getTenantRoleTable().iterator();
    while (roleTableIterator.hasNext()) {
      Table.KeyValue<String, String> keyValueRoleTable =
          roleTableIterator.next();
      String rolename = keyValueRoleTable.getValue();
      String accessId = keyValueRoleTable.getKey();
      OmDBAccessIdInfo accessIdInfo  =
          ozoneManager.getMetadataManager().getTenantAccessIdTable()
              .get(accessId);
      if (mtOMDBRoles.containsKey(rolename)) {
        HashSet<String> users = mtOMDBRoles.get(rolename);
        users.add(accessIdInfo.getUserPrincipal());
      } else {
        HashSet<String> users = new HashSet<>();
        users.add(accessIdInfo.getUserPrincipal());
        mtOMDBRoles.put(rolename, users);
      }
    }
  }

  public void processAllRolesFromOMDB() throws Exception {
    // Lets First make sure that all the Roles in OMDB are present in Ranger
    // as well as the corresponding userlist matches matches.
    for (String roleName: mtOMDBRoles.keySet()) {
      boolean pushRoleToRanger = false;
      if (mtRangerRoles.containsKey(roleName)) {
        HashSet<String> rangerUserList = mtRangerRoles.get(roleName).getUsers();
        for (String username : mtOMDBRoles.get(roleName)) {
          if (rangerUserList.contains(username)) {
            rangerUserList.remove(username);
            continue;
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
