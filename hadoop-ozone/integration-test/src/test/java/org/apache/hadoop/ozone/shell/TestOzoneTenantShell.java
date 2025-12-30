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

package org.apache.hadoop.ozone.shell;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTITENANCY_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_USER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMMultiTenantManagerImpl.OZONE_OM_TENANT_DEV_SKIP_RANGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.retry.RetryInvocationHandler;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.multitenant.AuthorizerLockImpl;
import org.apache.hadoop.ozone.om.request.s3.tenant.OMTenantAssignUserAccessIdRequest;
import org.apache.hadoop.ozone.om.request.s3.tenant.OMTenantCreateRequest;
import org.apache.hadoop.ozone.om.service.OMRangerBGSyncService;
import org.apache.hadoop.ozone.shell.tenant.TenantShell;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import picocli.CommandLine;

/**
 * Integration test for Ozone tenant shell command. HA enabled.
 *
 * TODO: HDDS-6338. Add a Kerberized version of this
 * TODO: HDDS-6336. Add a mock Ranger server to test Ranger HTTP endpoint calls
 */
public class TestOzoneTenantShell {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneTenantShell.class);

  static {
    System.setProperty("log4j.configurationFile", "auditlog.properties");
  }

  /**
   * Set the timeout for every test.
   */

  @TempDir
  private static Path path;
  private static final File AUDIT_LOG_FILE = new File("audit.log");

  private static OzoneConfiguration conf = null;
  private static MiniOzoneHAClusterImpl cluster = null;
  private static OzoneShell ozoneSh = null;
  private static TenantShell tenantShell = null;

  private final StringWriter out = new StringWriter();
  private final StringWriter err = new StringWriter();

  private static String omServiceId;
  private static int numOfOMs;

  private static final boolean USE_ACTUAL_RANGER = false;

  @BeforeAll
  public static void init() throws Exception {
    // Remove audit log output if it exists
    FileUtils.deleteQuietly(AUDIT_LOG_FILE);

    conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_OM_TENANT_DEV_SKIP_RANGER, true);
    conf.setBoolean(OZONE_OM_MULTITENANCY_ENABLED, true);

    if (USE_ACTUAL_RANGER) {
      conf.set(OZONE_RANGER_HTTPS_ADDRESS_KEY, System.getenv("RANGER_ADDRESS"));
      conf.set(OZONE_OM_RANGER_HTTPS_ADMIN_API_USER,
          System.getenv("RANGER_USER"));
      conf.set(OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD,
          System.getenv("RANGER_PASSWD"));
    } else {
      conf.setBoolean(OZONE_OM_TENANT_DEV_SKIP_RANGER, true);
    }

    File testFile = new File(path + OzoneConsts.OZONE_URI_DELIMITER + "testFile");
    FileUtils.touch(testFile);

    ozoneSh = new OzoneShell();
    tenantShell = new TenantShell();

    // Init cluster
    omServiceId = "om-service-test1";
    numOfOMs = 3;
    MiniOzoneHAClusterImpl.Builder builder = MiniOzoneCluster.newHABuilder(conf);
    builder.setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(numOfOMs)
        .withoutDatanodes();  // Remove this once we are actually writing data
    cluster = builder.build();
    cluster.waitForClusterToBeReady();
  }

  /**
   * shutdown MiniOzoneCluster.
   */
  @AfterAll
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }

    FileUtils.deleteQuietly(AUDIT_LOG_FILE);
  }

  @BeforeEach
  public void setup() throws UnsupportedEncodingException {
    tenantShell.getCmd().setOut(new PrintWriter(out));
    tenantShell.getCmd().setErr(new PrintWriter(err));
    ozoneSh.getCmd().setOut(new PrintWriter(out));
    ozoneSh.getCmd().setErr(new PrintWriter(err));
    // Suppress OMNotLeaderException in the log
    GenericTestUtils.setLogLevel(RetryInvocationHandler.class, Level.WARN);
    // Enable debug logging for interested classes
    GenericTestUtils.setLogLevel(OMTenantCreateRequest.class, Level.DEBUG);
    GenericTestUtils.setLogLevel(
        OMTenantAssignUserAccessIdRequest.class, Level.DEBUG);
    GenericTestUtils.setLogLevel(AuthorizerLockImpl.class, Level.DEBUG);

    GenericTestUtils.setLogLevel(OMRangerBGSyncService.class, Level.DEBUG);
  }

  /**
   * Returns exit code.
   */
  private int execute(GenericCli shell, String[] args) {
    LOG.info("Executing shell command with args {}", Arrays.asList(args));
    CommandLine cmd = shell.getCmd();
    CommandLine.IExecutionExceptionHandler exceptionHandler =
        (ex, commandLine, parseResult) -> {
          commandLine.getErr().println(ex.getMessage());
          return commandLine.getCommandSpec().exitCodeOnExecutionException();
        };

    // Since there is no elegant way to pass Ozone config to the shell,
    // the idea is to use 'set' to place those OM HA configs.
    String[] argsWithHAConf = getHASetConfStrings(args);

    cmd.setExecutionExceptionHandler(exceptionHandler);
    return cmd.execute(argsWithHAConf);
  }

  /**
   * Helper that appends HA service id to args.
   */
  private int executeHA(GenericCli shell, String[] args) {
    final String[] newArgs = new String[args.length + 1];
    System.arraycopy(args, 0, newArgs, 0, args.length);
    newArgs[args.length] = "--om-service-id=" + omServiceId;
    return execute(shell, newArgs);
  }

  private String getSetConfStringFromConf(String key) {
    return String.format("--set=%s=%s", key, conf.get(key));
  }

  private String generateSetConfString(String key, String value) {
    return String.format("--set=%s=%s", key, value);
  }

  /**
   * Helper function to get a String array to be fed into OzoneShell.
   * @param numOfArgs Additional number of arguments after the HA conf string,
   *                  this translates into the number of empty array elements
   *                  after the HA conf string.
   * @return String array.
   */
  private String[] getHASetConfStrings(int numOfArgs) {
    assert (numOfArgs >= 0);
    String[] res = new String[1 + 1 + numOfOMs + numOfArgs];
    final int indexOmServiceIds = 0;
    final int indexOmNodes = 1;
    final int indexOmAddressStart = 2;

    res[indexOmServiceIds] = getSetConfStringFromConf(
        OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY);

    String omNodesKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_NODES_KEY, omServiceId);
    String omNodesVal = conf.get(omNodesKey);
    res[indexOmNodes] = generateSetConfString(omNodesKey, omNodesVal);

    String[] omNodesArr = omNodesVal.split(",");
    // Sanity check
    assert (omNodesArr.length == numOfOMs);
    for (int i = 0; i < numOfOMs; i++) {
      res[indexOmAddressStart + i] =
          getSetConfStringFromConf(ConfUtils.addKeySuffixes(
              OMConfigKeys.OZONE_OM_ADDRESS_KEY, omServiceId, omNodesArr[i]));
    }

    return res;
  }

  /**
   * Helper function to create a new set of arguments that contains HA configs.
   * @param existingArgs Existing arguments to be fed into OzoneShell command.
   * @return String array.
   */
  private String[] getHASetConfStrings(String[] existingArgs) {
    // Get a String array populated with HA configs first
    String[] res = getHASetConfStrings(existingArgs.length);

    int indexCopyStart = res.length - existingArgs.length;
    // Then copy the existing args to the returned String array
    System.arraycopy(existingArgs, 0, res, indexCopyStart,
        existingArgs.length);
    return res;
  }

  /**
   * Helper function that checks command output AND clears it.
   */
  private void checkOutput(StringWriter writer, String stringToMatch,
                           boolean exactMatch) throws IOException {
    writer.flush();
    final String str = writer.toString();
    checkOutput(str, stringToMatch, exactMatch);
    writer.getBuffer().setLength(0);
  }

  private void checkOutput(StringWriter writer, String stringToMatch,
      boolean exactMatch, boolean expectValidJSON) throws IOException {
    writer.flush();
    final String str = writer.toString();
    if (expectValidJSON) {
      // Verify if the String can be parsed as a valid JSON
      final ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.readTree(str);
    }
    checkOutput(str, stringToMatch, exactMatch);
    writer.getBuffer().setLength(0);
  }

  private void checkOutput(String str, String stringToMatch,
                           boolean exactMatch) {
    if (exactMatch) {
      assertEquals(stringToMatch, str);
    } else {
      assertThat(str).contains(stringToMatch);
    }
  }

  private void deleteVolume(String volumeName) throws IOException {
    int exitC = execute(ozoneSh, new String[] {"volume", "delete", volumeName});
    checkOutput(out, "Volume " + volumeName + " is deleted\n", true);
    checkOutput(err, "", true);
    // Exit code should be 0
    assertEquals(0, exitC);
  }

  @Test
  public void testAssignAdmin() throws IOException {

    final String tenantName = "devaa";
    final String userName = "alice";

    executeHA(tenantShell, new String[] {"create", tenantName});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    // Loop assign-revoke 4 times
    for (int i = 0; i < 4; i++) {
      executeHA(tenantShell, new String[] {
          "user", "assign", userName, "--tenant=" + tenantName});
      checkOutput(out, "export AWS_ACCESS_KEY_ID=", false);
      checkOutput(err, "", true);

      executeHA(tenantShell, new String[] {"--verbose", "user", "assign-admin",
          tenantName + "$" + userName, "--tenant=" + tenantName,
          "--delegated=true"});
      checkOutput(out, "{\n" + "  \"accessId\" : \"devaa$alice\",\n"
          + "  \"tenantId\" : \"devaa\",\n" + "  \"isAdmin\" : true,\n"
          + "  \"isDelegatedAdmin\" : true\n" + "}\n", true, true);
      checkOutput(err, "", true);

      // Clean up
      executeHA(tenantShell, new String[] {"--verbose", "user", "revoke-admin",
          tenantName + "$" + userName, "--tenant=" + tenantName});
      checkOutput(out, "{\n" + "  \"accessId\" : \"devaa$alice\",\n"
          + "  \"tenantId\" : \"devaa\",\n" + "  \"isAdmin\" : false,\n"
          + "  \"isDelegatedAdmin\" : false\n" + "}\n", true, true);
      checkOutput(err, "", true);

      executeHA(tenantShell, new String[] {
          "user", "revoke", tenantName + "$" + userName});
      checkOutput(out, "", true);
      checkOutput(err, "", true);
    }

    // Clean up
    executeHA(tenantShell, new String[] {"delete", tenantName});
    checkOutput(out, "", true);
    checkOutput(err, "Deleted tenant '" + tenantName + "'.\n", false);
    deleteVolume(tenantName);

    // Sanity check: tenant list should be empty
    executeHA(tenantShell, new String[] {"list"});
    checkOutput(out, "", true);
    checkOutput(err, "", true);
  }

  /**
   * Test tenant create, assign user, get user info, assign admin, revoke admin
   * and revoke user flow.
   */
  @Test
  @SuppressWarnings("methodlength")
  public void testOzoneTenantBasicOperations() throws IOException {

    List<String> lines = FileUtils.readLines(AUDIT_LOG_FILE, (String)null);
    assertEquals(0, lines.size());

    executeHA(tenantShell, new String[] {"list"});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    // Create tenants
    // Equivalent to `ozone tenant create finance`
    executeHA(tenantShell, new String[] {"create", "finance"});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {"list"});
    checkOutput(out, "finance\n", true);
    checkOutput(err, "", true);

    lines = FileUtils.readLines(AUDIT_LOG_FILE, (String)null);
    assertThat(lines.size()).isGreaterThan(0);
    checkOutput(lines.get(lines.size() - 1), "ret=SUCCESS", false);

    // Check volume creation
    OmVolumeArgs volArgs = cluster.getOMLeader().getVolumeInfo("finance");
    assertEquals("finance", volArgs.getVolume());

    // Creating the tenant with the same name again should fail
    executeHA(tenantShell, new String[] {"create", "finance"});
    checkOutput(out, "", true);
    checkOutput(err, "Tenant 'finance' already exists\n", true);

    executeHA(tenantShell, new String[] {"create", "research"});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {"create", "dev"});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {"ls"});
    checkOutput(out, "dev\nfinance\nresearch\n", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {"list", "--json"});
    // Not checking the full output here
    checkOutput(out, "\"tenantId\" : \"dev\",", false);
    checkOutput(err, "", true);

    // Attempt user getsecret before assignment, should fail
    executeHA(tenantShell, new String[] {
        "user", "getsecret", "finance$bob"});
    checkOutput(out, "", false);
    checkOutput(err, "accessId 'finance$bob' doesn't exist\n",
        true);

    // Assign user accessId
    // Equivalent to `ozone tenant user assign bob --tenant=finance`
    executeHA(tenantShell, new String[] {
        "--verbose", "user", "assign", "bob", "--tenant=finance"});
    checkOutput(out, "export AWS_ACCESS_KEY_ID='finance$bob'\n"
        + "export AWS_SECRET_ACCESS_KEY='", false);
    checkOutput(err, "Assigned 'bob' to 'finance' with accessId"
        + " 'finance$bob'.\n", true);

    // Try user getsecret again after assignment, should succeed
    executeHA(tenantShell, new String[] {
        "user", "getsecret", "finance$bob"});
    checkOutput(out, "export AWS_ACCESS_KEY_ID='finance$bob'\n",
        false);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {
        "--verbose", "user", "assign", "bob", "--tenant=research"});
    checkOutput(out, "export AWS_ACCESS_KEY_ID='research$bob'\n"
        + "export AWS_SECRET_ACCESS_KEY='", false);
    checkOutput(err, "Assigned 'bob' to 'research' with accessId"
        + " 'research$bob'.\n", true);

    executeHA(tenantShell, new String[] {
        "user", "assign", "bob", "--tenant=dev"});
    checkOutput(out, "export AWS_ACCESS_KEY_ID='dev$bob'\n"
        + "export AWS_SECRET_ACCESS_KEY='", false);
    checkOutput(err, "", true);

    // accessId length exceeding limit, should fail
    executeHA(tenantShell, new String[] {
        "user", "assign", StringUtils.repeat('a', 100), "--tenant=dev"});
    checkOutput(out, "", true);
    checkOutput(err, "accessId length (104) exceeds the maximum length "
        + "allowed (100)\n", true);

    // Get user info
    // Equivalent to `ozone tenant user info bob`
    executeHA(tenantShell, new String[] {
        "user", "info", "bob"});
    checkOutput(out, "User 'bob' is assigned to:\n"
        + "- Tenant 'research' with accessId 'research$bob'\n"
        + "- Tenant 'finance' with accessId 'finance$bob'\n"
        + "- Tenant 'dev' with accessId 'dev$bob'\n", true);
    checkOutput(err, "", true);

    // Assign admin
    executeHA(tenantShell, new String[] {
        "user", "assign-admin", "dev$bob", "--tenant=dev", "--delegated"});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {
        "user", "info", "bob"});
    checkOutput(out, "Tenant 'dev' delegated admin with accessId", false);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {
        "user", "info", "--json", "bob"});
    checkOutput(out,
        "{\n" +
            "  \"user\" : \"bob\",\n" +
            "  \"tenants\" : [ {\n" +
            "    \"accessId\" : \"research$bob\",\n" +
            "    \"tenantId\" : \"research\",\n" +
            "    \"isAdmin\" : false,\n" +
            "    \"isDelegatedAdmin\" : false\n" +
            "  }, {\n" +
            "    \"accessId\" : \"finance$bob\",\n" +
            "    \"tenantId\" : \"finance\",\n" +
            "    \"isAdmin\" : false,\n" +
            "    \"isDelegatedAdmin\" : false\n" +
            "  }, {\n" +
            "    \"accessId\" : \"dev$bob\",\n" +
            "    \"tenantId\" : \"dev\",\n" +
            "    \"isAdmin\" : true,\n" +
            "    \"isDelegatedAdmin\" : true\n" +
            "  } ]\n" +
            "}\n",
        true, true);
    checkOutput(err, "", true);

    // Revoke admin
    executeHA(tenantShell, new String[] {
        "user", "revoke-admin", "dev$bob", "--tenant=dev"});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {
        "user", "info", "bob"});
    checkOutput(out, "User 'bob' is assigned to:\n"
        + "- Tenant 'research' with accessId 'research$bob'\n"
        + "- Tenant 'finance' with accessId 'finance$bob'\n"
        + "- Tenant 'dev' with accessId 'dev$bob'\n", true);
    checkOutput(err, "", true);

    // Revoke user accessId
    executeHA(tenantShell, new String[] {
        "user", "revoke", "research$bob"});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {
        "user", "info", "bob"});
    checkOutput(out, "User 'bob' is assigned to:\n"
        + "- Tenant 'finance' with accessId 'finance$bob'\n"
        + "- Tenant 'dev' with accessId 'dev$bob'\n", true);
    checkOutput(err, "", true);

    // Assign user again
    executeHA(tenantShell, new String[] {
        "user", "assign", "bob", "--tenant=research"});
    checkOutput(out, "export AWS_ACCESS_KEY_ID='research$bob'\n"
        + "export AWS_SECRET_ACCESS_KEY='", false);
    checkOutput(err, "", true);

    // Attempt to assign the user to the tenant again
    executeHA(tenantShell, new String[] {
        "user", "assign", "bob", "--tenant=research",
        "--accessId=research$bob"});
    checkOutput(out, "", false);
    checkOutput(err, "accessId 'research$bob' already exists!\n", true);

    // Attempt to assign the user to the tenant with a custom accessId
    executeHA(tenantShell, new String[] {
        "user", "assign", "bob", "--tenant=research",
        "--accessId=research$bob42"});
    checkOutput(out, "", false);
    // HDDS-6366: Disallow specifying custom accessId.
    checkOutput(err, "Invalid accessId 'research$bob42'. "
        + "Specifying a custom access ID disallowed. "
        + "Expected accessId to be assigned is 'research$bob'\n", true);

    executeHA(tenantShell, new String[] {"list"});
    checkOutput(out, "dev\nfinance\nresearch\n", true);
    checkOutput(err, "", true);

    // Clean up
    executeHA(tenantShell, new String[] {
        "user", "revoke", "research$bob"});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {"delete", "research"});
    checkOutput(out, "", true);
    checkOutput(err, "Deleted tenant 'research'.\n", false);
    deleteVolume("research");

    executeHA(tenantShell, new String[] {
        "user", "revoke", "finance$bob"});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {"list"});
    checkOutput(out, "dev\nfinance\n", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {"delete", "finance"});
    checkOutput(out, "", true);
    checkOutput(err, "Deleted tenant 'finance'.\n", false);
    deleteVolume("finance");

    executeHA(tenantShell, new String[] {"list"});
    checkOutput(out, "dev\n", true);
    checkOutput(err, "", true);

    // Attempt to delete tenant with accessIds still assigned to it, should fail
    int exitCode = executeHA(tenantShell, new String[] {"delete", "dev"});
    assertNotEquals(0, exitCode, "Tenant delete should fail!");
    checkOutput(out, "", true);
    checkOutput(err, "Tenant 'dev' is not empty. All accessIds associated "
        + "to this tenant must be revoked before the tenant can be deleted. "
        + "See `ozone tenant user revoke`\n", true);

    // Trigger BG sync on OMs to recover roles and policies deleted
    // by previous tenant delete attempt.
    // Note: Potential source of flakiness if triggered only on leader OM
    // in this case.
    // Because InMemoryMultiTenantAccessController is used in OMs for this
    // integration test, we need to trigger BG sync on all OMs just
    // in case a leader changed right after the last operation.
    cluster.getOzoneManagersList().forEach(om -> om.getMultiTenantManager()
        .getOMRangerBGSyncService().triggerRangerSyncOnce());

    // Delete dev volume should fail because the volume reference count > 0L
    exitCode = execute(ozoneSh, new String[] {"volume", "delete", "dev"});
    assertNotEquals(0, exitCode, "Volume delete should fail!");
    checkOutput(out, "", true);
    checkOutput(err, "Volume reference count is not zero (1). "
        + "Ozone features are enabled on this volume. "
        + "Try `ozone tenant delete <tenantId>` first.\n", true);

    executeHA(tenantShell, new String[] {"list"});
    checkOutput(out, "dev\n", true);
    checkOutput(err, "", true);

    // Revoke accessId first
    executeHA(tenantShell, new String[] {
        "user", "revoke", "dev$bob"});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    // Then delete tenant, should succeed
    executeHA(tenantShell, new String[] {"--verbose", "delete", "dev"});
    checkOutput(out, "{\n" + "  \"tenantId\" : \"dev\",\n"
            + "  \"volumeName\" : \"dev\",\n" + "  \"volumeRefCount\" : 0\n" + "}\n",
        true, true);
    checkOutput(err, "Deleted tenant 'dev'.\n", false);
    deleteVolume("dev");

    // Sanity check: tenant list should be empty
    executeHA(tenantShell, new String[] {"list"});
    checkOutput(out, "", true);
    checkOutput(err, "", true);
  }

  @Test
  public void testListTenantUsers() throws IOException {
    executeHA(tenantShell, new String[] {"--verbose", "create", "tenant1"});
    checkOutput(out, "{\n" +
        "  \"tenantId\" : \"tenant1\"\n" + "}\n", true, true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {
        "--verbose", "user", "assign", "alice", "--tenant=tenant1"});
    checkOutput(out, "export AWS_ACCESS_KEY_ID='tenant1$alice'\n"
        + "export AWS_SECRET_ACCESS_KEY='", false);
    checkOutput(err, "Assigned 'alice' to 'tenant1'" +
        " with accessId 'tenant1$alice'.\n", true);

    executeHA(tenantShell, new String[] {
        "user", "assign", "bob", "--tenant=tenant1"});
    checkOutput(out, "export AWS_ACCESS_KEY_ID='tenant1$bob'\n"
        + "export AWS_SECRET_ACCESS_KEY='", false);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {
        "user", "list", "tenant1"});
    checkOutput(out, "- User 'bob' with accessId 'tenant1$bob'\n" +
        "- User 'alice' with accessId 'tenant1$alice'\n", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {
        "user", "list", "tenant1", "--json"});
    checkOutput(out,
        "[ {\n" +
            "  \"user\" : \"bob\",\n" +
            "  \"accessId\" : \"tenant1$bob\"\n" +
            "}, {\n" +
            "  \"user\" : \"alice\",\n" +
            "  \"accessId\" : \"tenant1$alice\"\n" +
            "} ]\n", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {
        "user", "list", "tenant1", "--prefix=b"});
    checkOutput(out, "- User 'bob' with accessId " +
        "'tenant1$bob'\n", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {
        "user", "list", "tenant1", "--prefix=b", "--json"});
    checkOutput(out, "[ {\n" +
        "  \"user\" : \"bob\",\n" +
        "  \"accessId\" : \"tenant1$bob\"\n" +
        "} ]\n", true);
    checkOutput(err, "", true);

    int exitCode = executeHA(tenantShell, new String[] {
        "user", "list", "unknown"});
    assertNotEquals(0, exitCode, "Expected non-zero exit code");
    checkOutput(out, "", true);
    checkOutput(err, "Tenant 'unknown' doesn't exist.\n", true);

    // Clean up
    executeHA(tenantShell, new String[] {
        "user", "revoke", "tenant1$alice"});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {
        "--verbose", "user", "revoke", "tenant1$bob"});
    checkOutput(out, "", true);
    checkOutput(err, "Revoked accessId", false);

    executeHA(tenantShell, new String[] {"delete", "tenant1"});
    checkOutput(out, "", true);
    checkOutput(err, "Deleted tenant 'tenant1'.\n", false);
    deleteVolume("tenant1");

    // Sanity check: tenant list should be empty
    executeHA(tenantShell, new String[] {"list"});
    checkOutput(out, "", true);
    checkOutput(err, "", true);
  }

  @Test
  public void testTenantSetSecret() throws IOException, InterruptedException {

    final String tenantName = "tenant-test-set-secret";

    // Create test tenant
    executeHA(tenantShell, new String[] {"create", tenantName});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    // Set secret for non-existent accessId. Expect failure
    executeHA(tenantShell, new String[] {
        "user", "set-secret", tenantName + "$alice", "--secret=somesecret0"});
    checkOutput(out, "", true);
    checkOutput(err, "accessId '" + tenantName + "$alice' not found.\n", true);

    // Assign a user to the tenant so that we have an accessId entry
    executeHA(tenantShell, new String[] {
        "user", "assign", "alice", "--tenant=" + tenantName});
    checkOutput(out, "export AWS_ACCESS_KEY_ID='" + tenantName + "$alice'\n" +
        "export AWS_SECRET_ACCESS_KEY='", false);
    checkOutput(err, "", true);

    // Set secret as OM admin should succeed
    executeHA(tenantShell, new String[] {
        "user", "setsecret", tenantName + "$alice",
        "--secret=somesecret1"});
    checkOutput(out, "export AWS_ACCESS_KEY_ID='" + tenantName + "$alice'\n" +
        "export AWS_SECRET_ACCESS_KEY='somesecret1'\n", true);
    checkOutput(err, "", true);

    // Set empty secret key should fail
    int exitCode = executeHA(tenantShell, new String[] {
        "user", "setsecret", tenantName + "$alice",
        "--secret=short"});
    assertNotEquals(0, exitCode, "Expected non-zero exit code");
    checkOutput(out, "", true);
    checkOutput(err, "Secret key length should be at least 8 characters\n",
        true);

    // Get secret should still give the previous secret key
    executeHA(tenantShell, new String[] {
        "user", "getsecret", tenantName + "$alice"});
    checkOutput(out, "somesecret1", false);
    checkOutput(err, "", true);

    // Set secret as alice should succeed
    final UserGroupInformation ugiAlice = UserGroupInformation
        .createUserForTesting("alice",  new String[] {"usergroup"});

    ugiAlice.doAs((PrivilegedExceptionAction<Void>) () -> {
      executeHA(tenantShell, new String[] {
          "user", "setsecret", tenantName + "$alice",
          "--secret=somesecret2"});
      checkOutput(out, "export AWS_ACCESS_KEY_ID='" + tenantName + "$alice'\n" +
          "export AWS_SECRET_ACCESS_KEY='somesecret2'\n", true);
      checkOutput(err, "", true);
      return null;
    });

    // Set secret as bob should fail
    executeHA(tenantShell, new String[] {
        "user", "assign", "bob", "--tenant=" + tenantName});
    checkOutput(out, "export AWS_ACCESS_KEY_ID='" + tenantName + "$bob'\n" +
        "export AWS_SECRET_ACCESS_KEY='", false);
    checkOutput(err, "", true);

    final UserGroupInformation ugiBob = UserGroupInformation
        .createUserForTesting("bob",  new String[] {"usergroup"});

    ugiBob.doAs((PrivilegedExceptionAction<Void>) () -> {
      int exitC = executeHA(tenantShell, new String[] {
          "user", "setsecret", tenantName + "$alice",
          "--secret=somesecret2"});
      assertNotEquals(0, exitC, "Should return non-zero exit code!");
      checkOutput(out, "", true);
      checkOutput(err, "Requested accessId 'tenant-test-set-secret$alice'"
          + " doesn't belong to current user 'bob', nor does current user"
          + " have Ozone or tenant administrator privilege\n", true);
      return null;
    });

    // Once we make bob an admin of this tenant (non-delegated admin permission
    // is sufficient in this case), set secret should succeed
    executeHA(tenantShell, new String[] {"user", "assign-admin",
        tenantName + "$" + ugiBob.getShortUserName(),
        "--tenant=" + tenantName, "--delegated=false"});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    // Set secret should succeed now
    ugiBob.doAs((PrivilegedExceptionAction<Void>) () -> {
      executeHA(tenantShell, new String[] {
          "user", "setsecret", tenantName + "$alice",
          "--secret=somesecret2"});
      checkOutput(out, "export AWS_ACCESS_KEY_ID='" + tenantName + "$alice'\n" +
          "export AWS_SECRET_ACCESS_KEY='somesecret2'\n", true);
      checkOutput(err, "", true);
      return null;
    });

    // Clean up
    executeHA(tenantShell, new String[] {"user", "revoke-admin",
        tenantName + "$" + ugiBob.getShortUserName()});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {
        "user", "revoke", tenantName + "$bob"});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {
        "user", "revoke", tenantName + "$alice"});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {"delete", tenantName});
    checkOutput(out, "", true);
    checkOutput(err, "Deleted tenant '" + tenantName + "'.\n", false);
    deleteVolume(tenantName);

    // Sanity check: tenant list should be empty
    executeHA(tenantShell, new String[] {"list"});
    checkOutput(out, "", true);
    checkOutput(err, "", true);
  }

  @Test
  @SuppressWarnings("methodlength")
  public void testTenantAdminOperations()
      throws IOException, InterruptedException {

    final String tenantName = "tenant-test-tenant-admin-ops";
    final UserGroupInformation ugiAlice = UserGroupInformation
        .createUserForTesting("alice",  new String[] {"usergroup"});
    final UserGroupInformation ugiBob = UserGroupInformation
        .createUserForTesting("bob",  new String[] {"usergroup"});

    // Preparation

    // Create test tenant
    executeHA(tenantShell, new String[] {"create", tenantName});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    // Assign alice and bob as tenant users
    executeHA(tenantShell, new String[] {
        "user", "assign", "alice", "--tenant=" + tenantName});
    checkOutput(out, "export AWS_ACCESS_KEY_ID='" + tenantName + "$alice'\n" +
        "export AWS_SECRET_ACCESS_KEY='", false);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {
        "user", "assign", "bob", "--tenant=" + tenantName});
    checkOutput(out, "export AWS_ACCESS_KEY_ID='" + tenantName + "$bob'\n" +
        "export AWS_SECRET_ACCESS_KEY='", false);
    checkOutput(err, "", true);

    // Make alice a delegated tenant admin
    executeHA(tenantShell, new String[] {"user", "assign-admin",
        tenantName + "$" + ugiAlice.getShortUserName(),
        "--tenant=" + tenantName, "--delegated=true"});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    // Make bob a non-delegated tenant admin
    executeHA(tenantShell, new String[] {"user", "assign-admin",
        tenantName + "$" + ugiBob.getShortUserName(),
        "--tenant=" + tenantName, "--delegated=false"});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    // Start test matrix

    // As a tenant delegated admin, alice can:
    // - Assign and revoke user accessIds
    // - Assign and revoke tenant admins
    // - Set secret
    ugiAlice.doAs((PrivilegedExceptionAction<Void>) () -> {
      // Assign carol as a new tenant user
      executeHA(tenantShell, new String[] {
          "user", "assign", "carol", "--tenant=" + tenantName});
      checkOutput(out, "export AWS_ACCESS_KEY_ID='" + tenantName + "$carol'\n"
          + "export AWS_SECRET_ACCESS_KEY='", false);
      checkOutput(err, "", true);

      // Set secret should work
      executeHA(tenantShell, new String[] {
          "user", "setsecret", tenantName + "$alice",
          "--secret=somesecret2"});
      checkOutput(out, "export AWS_ACCESS_KEY_ID='" + tenantName + "$alice'\n" +
          "export AWS_SECRET_ACCESS_KEY='somesecret2'\n", true);
      checkOutput(err, "", true);

      // Make carol a tenant delegated tenant admin
      executeHA(tenantShell, new String[] {"user", "assign-admin",
          tenantName + "$carol",
          "--tenant=" + tenantName, "--delegated=true"});
      checkOutput(out, "", true);
      checkOutput(err, "", true);

      // Revoke carol's tenant admin privilege
      executeHA(tenantShell, new String[] {"user", "revoke-admin",
          tenantName + "$carol"});
      checkOutput(out, "", true);
      checkOutput(err, "", true);

      // Make carol a tenant non-delegated tenant admin
      executeHA(tenantShell, new String[] {"user", "assign-admin",
          tenantName + "$carol",
          "--tenant=" + tenantName, "--delegated=false"});
      checkOutput(out, "", true);
      checkOutput(err, "", true);

      // Revoke carol's tenant admin privilege
      executeHA(tenantShell, new String[] {"user", "revoke-admin",
          tenantName + "$carol"});
      checkOutput(out, "", true);
      checkOutput(err, "", true);

      // Revoke carol's accessId from this tenant
      executeHA(tenantShell, new String[] {
          "user", "revoke", tenantName + "$carol"});
      checkOutput(out, "", true);
      checkOutput(err, "", true);
      return null;
    });

    // As a tenant non-delegated admin, bob can:
    // - Assign and revoke user accessIds
    // - Set secret
    //
    // But bob can't:
    // - Assign and revoke tenant admins
    ugiBob.doAs((PrivilegedExceptionAction<Void>) () -> {
      // Assign carol as a new tenant user
      executeHA(tenantShell, new String[] {
          "user", "assign", "carol", "--tenant=" + tenantName});
      checkOutput(out, "export AWS_ACCESS_KEY_ID='" + tenantName + "$carol'\n"
          + "export AWS_SECRET_ACCESS_KEY='", false);
      checkOutput(err, "", true);

      // Set secret should work, even for a non-delegated admin
      executeHA(tenantShell, new String[] {
          "user", "setsecret", tenantName + "$alice",
          "--secret=somesecret2"});
      checkOutput(out, "export AWS_ACCESS_KEY_ID='" + tenantName + "$alice'\n" +
          "export AWS_SECRET_ACCESS_KEY='somesecret2'\n", true);
      checkOutput(err, "", true);

      // Attempt to make carol a tenant delegated tenant admin, should fail
      executeHA(tenantShell, new String[] {"user", "assign-admin",
          tenantName + "$carol",
          "--tenant=" + tenantName, "--delegated=true"});
      checkOutput(out, "", true);
      checkOutput(err, "User 'bob' is neither an Ozone admin "
          + "nor a delegated admin of tenant", false);

      // Attempt to make carol a tenant non-delegated tenant admin, should fail
      executeHA(tenantShell, new String[] {"user", "assign-admin",
          tenantName + "$carol",
          "--tenant=" + tenantName, "--delegated=false"});
      checkOutput(out, "", true);
      checkOutput(err, "User 'bob' is neither an Ozone admin "
          + "nor a delegated admin of tenant", false);

      // Attempt to revoke tenant admin, should fail at the permission check
      executeHA(tenantShell, new String[] {"user", "revoke-admin",
          tenantName + "$carol"});
      checkOutput(out, "", true);
      checkOutput(err, "User 'bob' is neither an Ozone admin "
          + "nor a delegated admin of tenant", false);

      // Revoke carol's accessId from this tenant
      executeHA(tenantShell, new String[] {
          "user", "revoke", tenantName + "$carol"});
      checkOutput(out, "", true);
      checkOutput(err, "", true);
      return null;
    });

    // Clean up
    executeHA(tenantShell, new String[] {"user", "revoke-admin",
        tenantName + "$" + ugiAlice.getShortUserName()});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {
        "user", "revoke", tenantName + "$" + ugiAlice.getShortUserName()});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {"user", "revoke-admin",
        tenantName + "$" + ugiBob.getShortUserName()});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {
        "user", "revoke", tenantName + "$" + ugiBob.getShortUserName()});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {"delete", tenantName});
    checkOutput(out, "", true);
    checkOutput(err, "Deleted tenant '" + tenantName + "'.\n", false);
    deleteVolume(tenantName);
  }

  @Test
  public void testCreateTenantOnExistingVolume() throws IOException {
    final String testVolume = "existing-volume-1";
    int exitC = execute(ozoneSh, new String[] {"volume", "create", testVolume});
    // Volume create should succeed
    assertEquals(0, exitC);
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    // Try to create tenant on the same volume, should fail by default
    executeHA(tenantShell, new String[] {"create", testVolume});
    checkOutput(out, "", true);
    checkOutput(err, "Volume already exists\n", true);

    // Try to create tenant on the same volume with --force, should work
    executeHA(tenantShell, new String[] {"create", testVolume, "--force"});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    // Try to create the same tenant one more time, should fail even
    // with --force because the tenant already exists.
    executeHA(tenantShell, new String[] {"create", testVolume, "--force"});
    checkOutput(out, "", true);
    checkOutput(err, "Tenant '" + testVolume + "' already exists\n", true);

    // Clean up
    executeHA(tenantShell, new String[] {"delete", testVolume});
    checkOutput(out, "", true);
    checkOutput(err, "Deleted tenant '" + testVolume + "'.\n", false);
    deleteVolume(testVolume);
  }
}
