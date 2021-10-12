/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.shell;

import com.google.common.base.Strings;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.retry.RetryInvocationHandler;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMultiTenantManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.s3.tenant.OMAssignUserToTenantRequest;
import org.apache.hadoop.ozone.om.request.s3.tenant.OMTenantCreateRequest;
import org.apache.hadoop.ozone.shell.tenant.TenantShell;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.fail;

/**
 * Integration test for Ozone tenant shell command. HA enabled.
 */
public class TestOzoneTenantShell {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneTenantShell.class);

  static {
    System.setProperty("log4j.configurationFile", "auditlog.properties");
    System.setProperty("log4j2.contextSelector",
        "org.apache.logging.log4j.core.async.AsyncLoggerContextSelector");
  }

  private static final String DEFAULT_ENCODING = UTF_8.name();

  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = Timeout.seconds(300);

  private static File baseDir;
  private static File testFile;
  private static final File AUDIT_LOG_FILE = new File("audit.log");

  private static OzoneConfiguration conf = null;
  private static MiniOzoneCluster cluster = null;
  private static TenantShell tenantShell = null;

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;

  private static String omServiceId;
  private static String clusterId;
  private static String scmId;
  private static int numOfOMs;

  /**
   * Create a MiniOzoneCluster for testing with using distributed Ozone
   * handler type.
   *
   * @throws Exception
   */
  @BeforeClass
  public static void init() throws Exception {
    // Remove audit log output if it exists
    if (AUDIT_LOG_FILE.exists()) {
      AUDIT_LOG_FILE.delete();
    }

    conf = new OzoneConfiguration();

    conf.setBoolean(
        OMMultiTenantManagerImpl.OZONE_OM_TENANT_DEV_SKIP_RANGER, true);

    String path = GenericTestUtils.getTempPath(
        TestOzoneTenantShell.class.getSimpleName());
    baseDir = new File(path);
    baseDir.mkdirs();

    testFile = new File(path + OzoneConsts.OZONE_URI_DELIMITER + "testFile");
    testFile.getParentFile().mkdirs();
    testFile.createNewFile();

    tenantShell = new TenantShell();

    // Init cluster
    omServiceId = "om-service-test1";
    numOfOMs = 3;
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    cluster = MiniOzoneCluster.newOMHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(numOfOMs)
        .build();
    cluster.waitForClusterToBeReady();
//    MiniOzoneHAClusterImpl impl = (MiniOzoneOMHAClusterImpl) cluster;
  }

  /**
   * shutdown MiniOzoneCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }

    if (baseDir != null) {
      FileUtil.fullyDelete(baseDir, true);
    }
  }

  @Before
  public void setup() throws UnsupportedEncodingException {
    System.setOut(new PrintStream(out, false, UTF_8.name()));
    System.setErr(new PrintStream(err, false, UTF_8.name()));
  }

  @After
  public void reset() {
    // reset stream after each unit test
    out.reset();
    err.reset();

    // restore system streams
    System.setOut(OLD_OUT);
    System.setErr(OLD_ERR);
  }

  private void execute(GenericCli shell, String[] args) {
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
    cmd.execute(argsWithHAConf);
  }

  /**
   * Helper that appends HA service id to args.
   */
  private void executeHA(GenericCli shell, String[] args) {
    final String[] newArgs = new String[args.length + 1];
    System.arraycopy(args, 0, newArgs, 0, args.length);
    newArgs[args.length] = "--om-service-id=" + omServiceId;
    execute(shell, newArgs);
  }

  /**
   * Execute command, assert exception message and returns true if error
   * was thrown.
   */
  private void executeWithError(OzoneShell shell, String[] args,
      String expectedError) {
    if (Strings.isNullOrEmpty(expectedError)) {
      execute(shell, args);
    } else {
      try {
        execute(shell, args);
        fail("Exception is expected from command execution " + Arrays
            .asList(args));
      } catch (Exception ex) {
        if (!Strings.isNullOrEmpty(expectedError)) {
          Throwable exceptionToCheck = ex;
          if (exceptionToCheck.getCause() != null) {
            exceptionToCheck = exceptionToCheck.getCause();
          }
          Assert.assertTrue(
              String.format(
                  "Error of OzoneShell code doesn't contain the " +
                      "exception [%s] in [%s]",
                  expectedError, exceptionToCheck.getMessage()),
              exceptionToCheck.getMessage().contains(expectedError));
        }
      }
    }
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
    assert(numOfArgs >= 0);
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
    assert(omNodesArr.length == numOfOMs);
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
  private void checkOutput(ByteArrayOutputStream stream, String stringToMatch,
      boolean exactMatch) throws IOException {
    stream.flush();
    final String str = stream.toString(DEFAULT_ENCODING);
    checkOutput(str, stringToMatch, exactMatch);
    stream.reset();
  }

  private void checkOutput(String str, String stringToMatch,
      boolean exactMatch) {
    if (exactMatch) {
      Assert.assertEquals(stringToMatch, str);
    } else {
      Assert.assertTrue(str.contains(stringToMatch));
    }
  }

  /**
   * Test tenant create, assign user, get user info, assign admin, revoke admin
   * and revoke user.
   */
  @Test
  public void testOzoneTenantBasicOperations() throws IOException {

    // Suppress OMNotLeaderException in the log
    GenericTestUtils.setLogLevel(RetryInvocationHandler.LOG, Level.WARN);

    GenericTestUtils.setLogLevel(OMTenantCreateRequest.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(OMAssignUserToTenantRequest.LOG, Level.DEBUG);

    List<String> lines = FileUtils.readLines(AUDIT_LOG_FILE, (String)null);
    Assert.assertEquals(0, lines.size());

    executeHA(tenantShell, new String[] {"list"});
    checkOutput(out, "", true);
    checkOutput(err, "", true);

    // Create tenants
    // Equivalent to `ozone tenant create finance`
    executeHA(tenantShell, new String[] {"create", "finance"});
    checkOutput(out, "Created tenant 'finance'.\n", true);
    checkOutput(err, "", true);

//    lines = FileUtils.readLines(AUDIT_LOG_FILE, (String)null);
    // FIXME: Below check is somewhat unstable.
    //  Occasionally lines.size() == 0 -> ArrayIndexOutOfBoundsException
    //  Possibly due to audit log not flushed.
//    checkOutput(lines.get(lines.size() - 1), "ret=SUCCESS", false);

    // Check volume creation
    OmVolumeArgs volArgs = cluster.getOzoneManager().getVolumeInfo("finance");
    Assert.assertEquals("finance", volArgs.getVolume());

    // Creating the tenant with the same name again should fail
    executeHA(tenantShell, new String[] {"create", "finance"});
    checkOutput(out, "", true);
    checkOutput(err, "Failed to create tenant 'finance':"
        + " Tenant already exists\n", true);

    executeHA(tenantShell, new String[] {"create", "research"});
    checkOutput(out, "Created tenant 'research'.\n", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {"create", "dev"});
    checkOutput(out, "Created tenant 'dev'.\n", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {"ls"});
    checkOutput(out, "dev\nfinance\nresearch\n", true);
    checkOutput(err, "", true);

    executeHA(tenantShell, new String[] {"list", "--long", "--header"});
    // Not checking the entire output here yet
    checkOutput(out, "Policy", false);
    checkOutput(err, "", true);

    // Assign user accessId
    // Equivalent to `ozone tenant user assign bob@EXAMPLE.COM --tenant=finance`
    executeHA(tenantShell, new String[] {
        "user", "assign", "bob@EXAMPLE.COM", "--tenant=finance"});
    checkOutput(out, "export AWS_ACCESS_KEY_ID='finance$bob@EXAMPLE.COM'\n"
        + "export AWS_SECRET_ACCESS_KEY='", false);
    checkOutput(err, "Assigned 'bob@EXAMPLE.COM' to 'finance' with accessId"
        + " 'finance$bob@EXAMPLE.COM'.\n", true);

    executeHA(tenantShell, new String[] {
        "user", "assign", "bob@EXAMPLE.COM", "--tenant=research"});
    checkOutput(out, "export AWS_ACCESS_KEY_ID='research$bob@EXAMPLE.COM'\n"
        + "export AWS_SECRET_ACCESS_KEY='", false);
    checkOutput(err, "Assigned 'bob@EXAMPLE.COM' to 'research' with accessId"
        + " 'research$bob@EXAMPLE.COM'.\n", true);

    executeHA(tenantShell, new String[] {
        "user", "assign", "bob@EXAMPLE.COM", "--tenant=dev"});
    checkOutput(out, "export AWS_ACCESS_KEY_ID='dev$bob@EXAMPLE.COM'\n"
        + "export AWS_SECRET_ACCESS_KEY='", false);
    checkOutput(err, "Assigned 'bob@EXAMPLE.COM' to 'dev' with accessId"
        + " 'dev$bob@EXAMPLE.COM'.\n", true);

    // Get user info
    // Equivalent to `ozone tenant user info bob@EXAMPLE.COM`
    executeHA(tenantShell, new String[] {
        "user", "info", "bob@EXAMPLE.COM"});
    checkOutput(out, "User 'bob@EXAMPLE.COM' is assigned to:\n"
        + "- Tenant 'finance' with accessId 'finance$bob@EXAMPLE.COM'\n"
        + "- Tenant 'research' with accessId 'research$bob@EXAMPLE.COM'\n"
        + "- Tenant 'dev' with accessId 'dev$bob@EXAMPLE.COM'\n\n", true);
    checkOutput(err, "", true);

    // Assign admin
    executeHA(tenantShell, new String[] {
        "user", "assignadmin", "dev$bob@EXAMPLE.COM", "--tenant=dev"});
    checkOutput(out, "", true);
    checkOutput(err,
        "Assigned admin to 'dev$bob@EXAMPLE.COM' in tenant 'dev'\n", true);

    executeHA(tenantShell, new String[] {
        "user", "info", "bob@EXAMPLE.COM"});
    checkOutput(out, "Tenant 'dev' delegated admin with accessId", false);
    checkOutput(err, "", true);

    // Assign admin
    executeHA(tenantShell, new String[] {
        "user", "revokeadmin", "dev$bob@EXAMPLE.COM", "--tenant=dev"});
    checkOutput(out, "", true);
    checkOutput(err, "Revoked admin role of 'dev$bob@EXAMPLE.COM' "
        + "from tenant 'dev'\n", true);

    executeHA(tenantShell, new String[] {
        "user", "info", "bob@EXAMPLE.COM"});
    checkOutput(out, "User 'bob@EXAMPLE.COM' is assigned to:\n"
        + "- Tenant 'finance' with accessId 'finance$bob@EXAMPLE.COM'\n"
        + "- Tenant 'research' with accessId 'research$bob@EXAMPLE.COM'\n"
        + "- Tenant 'dev' with accessId 'dev$bob@EXAMPLE.COM'\n\n", true);
    checkOutput(err, "", true);

    // Revoke user accessId
    executeHA(tenantShell, new String[] {
        "user", "revoke", "research$bob@EXAMPLE.COM"});
    checkOutput(out, "", true);
    checkOutput(err, "Revoked accessId 'research$bob@EXAMPLE.COM'.\n", true);

    executeHA(tenantShell, new String[] {
        "user", "info", "bob@EXAMPLE.COM"});
    checkOutput(out, "User 'bob@EXAMPLE.COM' is assigned to:\n"
        + "- Tenant 'finance' with accessId 'finance$bob@EXAMPLE.COM'\n"
        + "- Tenant 'dev' with accessId 'dev$bob@EXAMPLE.COM'\n\n", true);
    checkOutput(err, "", true);
  }

}
