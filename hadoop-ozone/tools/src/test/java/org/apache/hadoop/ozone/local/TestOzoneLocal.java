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

package org.apache.hadoop.ozone.local;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IDefaultValueProvider;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.Model.OptionSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;

/**
 * Tests for {@link OzoneLocal}.
 */
class TestOzoneLocal {

  @Test
  void localCommandMetadataIsPresentAndPublic() {
    Command command = OzoneLocal.class.getAnnotation(Command.class);

    assertNotNull(command);
    assertEquals("ozone local", command.name());
    assertFalse(command.hidden());
  }

  @Test
  void runCommandMetadataIsPresentAndPublic() {
    Command command = OzoneLocal.RunCommand.class.getAnnotation(Command.class);

    assertNotNull(command);
    assertEquals("run", command.name());
    assertFalse(command.hidden());
  }

  @Test
  void genericCliRegistersRunCommand() {
    OzoneLocal local = new OzoneLocal();

    assertTrue(local.getCmd().getSubcommands().containsKey("run"));
  }

  @Test
  void rootHelpListsRunCommand() throws Exception {
    OzoneLocal local = new OzoneLocal();
    CommandLine commandLine = local.getCmd();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    commandLine.setOut(new PrintWriter(new OutputStreamWriter(out, UTF_8),
        true));
    commandLine.setErr(new PrintWriter(new OutputStreamWriter(err, UTF_8),
        true));

    int exitCode = local.execute(new String[] {"--help"});

    String help = out.toString(UTF_8.name());
    assertEquals(0, exitCode);
    assertTrue(help.contains("Usage: ozone local"));
    assertTrue(help.matches("(?s).*\\R\\s+run\\b.*"), help);
    assertEquals("", err.toString(UTF_8.name()));
  }

  @Test
  void runCommandStartsRuntimeAndPrintsStartupSummary() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    StubRuntime runtime = new StubRuntime("localhost", 9860, 9862, "http://localhost:9878");
    TestableRunCommand command = new TestableRunCommand(runtime);
    CommandLine commandLine = new CommandLine(command);
    commandLine.setOut(new PrintWriter(new OutputStreamWriter(out, UTF_8),
        true));

    int exitCode = commandLine.execute();

    assertEquals(0, exitCode);
    assertTrue(runtime.started);
    assertTrue(runtime.closed);
    String text = out.toString(UTF_8.name());
    assertTrue(text.contains("Local Ozone is running from"), text);
    assertTrue(text.contains("SCM RPC: localhost:9860"), text);
    assertTrue(text.contains("OM RPC: localhost:9862"), text);
    assertFalse(text.contains("Datanodes:"), text);
    assertTrue(text.contains("S3 endpoint: http://localhost:9878"), text);
    assertTrue(text.contains("AWS_ACCESS_KEY_ID=" + LocalOzoneClusterConfig.LOCAL_S3_ACCESS_KEY), text);
    assertTrue(text.contains("AWS_SECRET_ACCESS_KEY=" + LocalOzoneClusterConfig.LOCAL_S3_SECRET_KEY), text);
    assertTrue(text.contains("AWS_REGION=" + LocalOzoneClusterConfig.LOCAL_S3_REGION), text);
    assertTrue(text.contains("AWS_ENDPOINT_URL_S3=http://localhost:9878"), text);
    assertTrue(text.contains("aws configure set default.s3.addressing_style path"), text);
    // The printed pair is an example, not a credential the gateway enforces. Saying so is what
    // keeps a reader from treating the local endpoint as access-controlled.
    assertTrue(text.contains("accepts any credentials"), text);
    assertTrue(text.contains("Press Ctrl+C to stop."), text);
  }

  @Test
  void runCommandPrintsReconEndpointWhenEnabled() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    StubRuntime runtime = new StubRuntime("localhost", 9860, 9862,
        "http://localhost:9878");
    runtime.reconEndpoint = "http://localhost:9888";
    TestableRunCommand command = new TestableRunCommand(runtime);
    CommandLine commandLine = new CommandLine(command);
    commandLine.setOut(new PrintWriter(new OutputStreamWriter(out, UTF_8),
        true));

    int exitCode = commandLine.execute("--recon");

    assertEquals(0, exitCode);
    String text = out.toString(UTF_8.name());
    assertTrue(text.contains("Recon endpoint: http://localhost:9888"), text);
  }

  @Test
  void runCommandOmitsReconEndpointWhenDisabled() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    StubRuntime runtime = new StubRuntime("localhost", 9860, 9862,
        "http://localhost:9878");
    TestableRunCommand command = new TestableRunCommand(runtime);
    CommandLine commandLine = new CommandLine(command);
    commandLine.setOut(new PrintWriter(new OutputStreamWriter(out, UTF_8),
        true));

    int exitCode = commandLine.execute();

    assertEquals(0, exitCode);
    assertFalse(out.toString(UTF_8.name()).contains("Recon endpoint:"));
  }

  @Test
  void runCommandOmitsS3SummaryWhenS3gDisabled() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    StubRuntime runtime = new StubRuntime("localhost", 9860, 9862, "");
    TestableRunCommand command = new TestableRunCommand(runtime);
    CommandLine commandLine = new CommandLine(command);
    commandLine.setOut(new PrintWriter(new OutputStreamWriter(out, UTF_8),
        true));

    int exitCode = commandLine.execute("--no-s3g");

    assertEquals(0, exitCode);
    String text = out.toString(UTF_8.name());
    assertFalse(text.contains("S3 endpoint:"), text);
    assertFalse(text.contains("AWS_ACCESS_KEY_ID="), text);
    assertTrue(text.contains("Press Ctrl+C to stop."), text);
  }

  @Test
  void runCommandClosesRuntimeWhenStartupFails() {
    StubRuntime runtime = new StubRuntime("localhost", 9860, 9862, "");
    runtime.failStart = true;
    TestableRunCommand command = new TestableRunCommand(runtime);

    int exitCode = new CommandLine(command).execute();

    assertEquals(1, exitCode);
    assertTrue(runtime.closed);
  }

  /** The marker preserves the original cause for the root command's error handling. */
  @Test
  void runCommandPreservesStartupFailureAsTheCause() throws Exception {
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    StubRuntime runtime = new StubRuntime("localhost", 9860, 9862, "");
    runtime.failStart = true;
    TestableRunCommand command = new TestableRunCommand(runtime);
    CommandLine commandLine = new CommandLine(command);
    commandLine.setErr(new PrintWriter(new OutputStreamWriter(err, UTF_8), true));
    commandLine.parseArgs();

    Exception error = assertThrows(Exception.class, command::call);

    assertTrue(error.getCause() instanceof IllegalStateException, error.toString());
    assertEquals("startup failed", error.getCause().getMessage());
    assertEquals("", err.toString(UTF_8.name()));
    assertTrue(runtime.closed);
  }

  /**
   * Drives the whole path a user hits: the cluster rejects the value and OzoneLocal prints the
   * rejection before a hint on the next line.
   * The timeout is preemptive because execute() otherwise blocks in awaitShutdown() until a JVM
   * shutdown hook fires, so a check that stopped rejecting would hang the fork instead of failing.
   */
  @Test
  void conflictingConfigReachesStderrThroughGenericCli(@TempDir Path dataDir) throws Exception {
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    OzoneLocal ozoneLocal = new OzoneLocal();
    ozoneLocal.getCmd().setErr(new PrintWriter(new OutputStreamWriter(err, UTF_8), true));

    int exitCode = assertTimeoutPreemptively(Duration.ofSeconds(30),
        () -> ozoneLocal.execute(new String[] {"-D", OZONE_REPLICATION + "=THREE",
            "run", "--data-dir", dataDir.toString()}));

    assertEquals(GenericCli.EXECUTION_ERROR_EXIT_CODE, exitCode);
    String[] lines = err.toString(UTF_8.name()).trim().split("\n");
    assertEquals(2, lines.length, err.toString(UTF_8.name()));
    assertTrue(lines[0].contains(OZONE_REPLICATION), lines[0]);
    assertTrue(lines[0].contains("THREE"), lines[0]);
    assertTrue(lines[1].contains("--verbose"), lines[1]);
  }

  @Test
  void relativeDefaultNamedConfigFileCountsAsUserConfig(@TempDir Path tempDir) throws Exception {
    Path customConfig = tempDir.resolve("ozone-default.xml").toAbsolutePath();
    Files.write(customConfig, ("<configuration><property><name>" + OZONE_REPLICATION
        + "</name><value>THREE</value></property></configuration>").getBytes(UTF_8));
    Path relativeConfig = Paths.get("").toAbsolutePath().relativize(customConfig);
    OzoneLocal ozoneLocal = new OzoneLocal();

    ozoneLocal.getCmd().parseArgs("--conf", relativeConfig.toString(), "run");

    OzoneConfiguration seed = ozoneLocal.getOzoneConf();
    assertEquals("THREE", seed.get(OZONE_REPLICATION));
    String[] sources = seed.getPropertySources(OZONE_REPLICATION);
    String source = sources[sources.length - 1];
    assertEquals(customConfig.normalize().toString(), source);
    LocalOzoneClusterConfig config = LocalOzoneClusterConfig.builder(
        tempDir.resolve("local-ozone")).build();

    IOException error = assertThrows(IOException.class, () -> {
      try (LocalOzoneCluster cluster = new LocalOzoneCluster(config, seed)) {
        cluster.prepareConfiguration();
      }
    });

    assertTrue(error.getMessage().contains(source), error.getMessage());
  }

  /**
   * A --conf value carrying a scheme is a Hadoop fs.Path resource, not a local file name;
   * absolutizing it through java.nio mangles it into a CWD-relative literal path that
   * Configuration then skips silently, ignoring the user's file.
   */
  @Test
  void fileUriConfigPathLoadsTheNamedFile(@TempDir Path tempDir) throws Exception {
    Path customConfig = tempDir.resolve("my-ozone-site.xml").toAbsolutePath();
    Files.write(customConfig, ("<configuration><property><name>" + OZONE_REPLICATION
        + "</name><value>THREE</value></property></configuration>").getBytes(UTF_8));
    OzoneLocal ozoneLocal = new OzoneLocal();

    ozoneLocal.getCmd().parseArgs("--conf", customConfig.toUri().toString(), "run");

    assertEquals("THREE", ozoneLocal.getOzoneConf().get(OZONE_REPLICATION));
  }

  /**
   * An empty --conf (an unset shell variable, say) has to fail at option parse the way
   * fs.Path rejects it, not resolve to the working directory and die later with an
   * unrelated resource error.
   */
  @Test
  void emptyConfigPathIsRejectedAtParse() {
    OzoneLocal ozoneLocal = new OzoneLocal();

    assertThrows(CommandLine.ParameterException.class,
        () -> ozoneLocal.getCmd().parseArgs("--conf", "", "run"));
  }

  @Test
  void startupFailureHintOnlySuggestsMissingDetail() {
    String noDetail = OzoneLocal.startupFailureHint(false, false);
    assertTrue(noDetail.contains("--loglevel INFO"), noDetail);
    assertTrue(noDetail.contains("--verbose"), noDetail);

    String logsEnabled = OzoneLocal.startupFailureHint(true, false);
    assertFalse(logsEnabled.contains("--loglevel INFO"), logsEnabled);
    assertTrue(logsEnabled.contains("--verbose"), logsEnabled);

    String verboseEnabled = OzoneLocal.startupFailureHint(false, true);
    assertTrue(verboseEnabled.contains("--loglevel INFO"), verboseEnabled);
    assertFalse(verboseEnabled.contains("--verbose"), verboseEnabled);

    assertNull(OzoneLocal.startupFailureHint(true, true));
  }

  @Test
  void runCommandOptionsUseEnvironmentDefaults() throws Exception {
    assertEnvDefault("dataDir", OzoneLocal.ENV_DATA_DIR,
        LocalOzoneClusterConfig.DEFAULT_DATA_DIR_VALUE);
    assertEnvDefault("formatMode", OzoneLocal.ENV_FORMAT,
        LocalOzoneClusterConfig.DEFAULT_FORMAT_MODE_VALUE);
    assertEnvDefault("datanodes", OzoneLocal.ENV_DATANODES,
        LocalOzoneClusterConfig.DEFAULT_DATANODES_VALUE);
    assertEnvDefault("host", OzoneLocal.ENV_HOST,
        LocalOzoneClusterConfig.DEFAULT_HOST);
    assertEnvDefault("bindHost", OzoneLocal.ENV_BIND_HOST,
        LocalOzoneClusterConfig.DEFAULT_BIND_HOST);
    assertEnvDefault("scmPort", OzoneLocal.ENV_SCM_PORT,
        LocalOzoneClusterConfig.DEFAULT_PORT_VALUE);
    assertEnvDefault("omPort", OzoneLocal.ENV_OM_PORT,
        LocalOzoneClusterConfig.DEFAULT_PORT_VALUE);
    assertEnvDefault("s3gEnabled", OzoneLocal.ENV_S3G_ENABLED,
        LocalOzoneClusterConfig.DEFAULT_S3G_ENABLED_VALUE);
    assertEnvDefault("s3gPort", OzoneLocal.ENV_S3G_PORT,
        LocalOzoneClusterConfig.DEFAULT_PORT_VALUE);
    assertEnvDefault("reconEnabled", OzoneLocal.ENV_RECON_ENABLED,
        LocalOzoneClusterConfig.DEFAULT_RECON_ENABLED_VALUE);
    assertEnvDefault("reconPort", OzoneLocal.ENV_RECON_PORT,
        LocalOzoneClusterConfig.DEFAULT_PORT_VALUE);
    assertEnvDefault("ephemeral", OzoneLocal.ENV_EPHEMERAL,
        LocalOzoneClusterConfig.DEFAULT_EPHEMERAL_VALUE);
    assertEnvDefault("startupTimeout", OzoneLocal.ENV_STARTUP_TIMEOUT,
        LocalOzoneClusterConfig.DEFAULT_STARTUP_TIMEOUT_VALUE);
  }

  @Test
  void resolveConfigUsesPicocliDefaults() {
    LocalOzoneClusterConfig config = resolveWithFallbackDefaults();

    assertEquals(LocalOzoneClusterConfig.DEFAULT_DATA_DIR,
        config.getDataDir());
    assertEquals(LocalOzoneClusterConfig.FormatMode.IF_NEEDED,
        config.getFormatMode());
    assertEquals(1, config.getDatanodes());
    assertEquals("127.0.0.1", config.getHost());
    assertEquals("127.0.0.1", config.getBindHost());
    assertEquals(0, config.getScmPort());
    assertEquals(0, config.getOmPort());
    assertEquals(0, config.getS3gPort());
    assertTrue(config.isS3gEnabled());
    assertEquals(0, config.getReconPort());
    assertFalse(config.isReconEnabled());
    assertFalse(config.isEphemeral());
    assertEquals(Duration.ofMinutes(2), config.getStartupTimeout());
  }

  @Test
  void resolveConfigUsesCliOverrides() {
    LocalOzoneClusterConfig config = resolve(
        "--data-dir", "target/cli-local",
        "--format", "always",
        "--datanodes", "3",
        "--host", "cli-host",
        "--bind-host", "0.0.0.0",
        "--scm-port", "200",
        "--om-port", "201",
        "--s3g-port", "202",
        "--no-s3g",
        "--recon-port", "203",
        "--recon",
        "--ephemeral",
        "--startup-timeout", "45s");

    assertEquals(Paths.get("target/cli-local").toAbsolutePath().normalize(),
        config.getDataDir());
    assertEquals(LocalOzoneClusterConfig.FormatMode.ALWAYS,
        config.getFormatMode());
    assertEquals(3, config.getDatanodes());
    assertEquals("cli-host", config.getHost());
    assertEquals("0.0.0.0", config.getBindHost());
    assertEquals(200, config.getScmPort());
    assertEquals(201, config.getOmPort());
    assertEquals(202, config.getS3gPort());
    assertFalse(config.isS3gEnabled());
    assertEquals(203, config.getReconPort());
    assertTrue(config.isReconEnabled());
    assertTrue(config.isEphemeral());
    assertEquals(Duration.ofSeconds(45), config.getStartupTimeout());
  }

  @Test
  void resolveConfigParsesIsoStartupTimeout() {
    LocalOzoneClusterConfig config = resolve("--startup-timeout", "PT45S");

    assertEquals(Duration.ofSeconds(45), config.getStartupTimeout());
  }

  @Test
  void resolveConfigAllowsS3gAndEphemeralToBeNegated() {
    LocalOzoneClusterConfig config = resolve("--s3g", "--no-ephemeral");

    assertTrue(config.isS3gEnabled());
    assertFalse(config.isEphemeral());
  }

  @Test
  void resolveConfigAllowsReconToBeNegated() {
    LocalOzoneClusterConfig config = resolve("--no-recon");

    assertFalse(config.isReconEnabled());
  }

  @Test
  void resolveConfigRejectsInvalidReconPort() {
    assertConfigError("--recon-port", "65536", "--recon-port");
  }

  @Test
  void resolveConfigRejectsInvalidFormat() {
    assertParseError("--format", "sometimes", "sometimes");
  }

  @Test
  void resolveConfigRejectsInvalidInteger() {
    assertParseError("--datanodes", "two", "--datanodes");
  }

  @Test
  void resolveConfigRejectsInvalidPort() {
    assertConfigError("--scm-port", "65536", "--scm-port");
  }

  @Test
  void resolveConfigRejectsDatanodeCountBelowOne() {
    assertConfigError("--datanodes", "0", "--datanodes");
  }

  /**
   * Pins the value echo: picocli's wrapper already names the option, so asserting on the
   * option alone would pass even if the converter dropped the value from its message.
   */
  @Test
  void resolveConfigRejectsInvalidDuration() {
    assertParseError("--startup-timeout", "forever", "Invalid duration 'forever'");
  }

  @Test
  void resolveConfigRejectsNonPositiveDuration() {
    assertConfigError("--startup-timeout", "0s", "--startup-timeout");
  }

  /**
   * Without the unit check this parses as 120 milliseconds, so the run dies with an unrelated
   * timeout instead of telling the user the value was misread. Asserts the quoted value: the
   * bare digits also occur in the static "like 120s" hint, which would mask a dropped echo.
   */
  @Test
  void resolveConfigRejectsDurationWithoutTimeUnit() {
    assertParseError("--startup-timeout", "120", "Missing time unit in '120'");
  }

  @Test
  void resolveConfigAcceptsHadoopStyleMinutes() {
    assertEquals(Duration.ofMinutes(2), resolve("--startup-timeout", "2m")
        .getStartupTimeout());
  }

  @Test
  void resolveConfigRejectsInvalidPath() {
    assertParseError("--data-dir", "\0", "--data-dir");
  }

  @Test
  void legacyWithoutS3gOptionIsNotAccepted() {
    assertParseError("--without-s3g", "--without-s3g");
  }

  @Test
  void genericCliErrorOutputIncludesOffendingConfigSource()
      throws Exception {
    OzoneLocal local = new OzoneLocal();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    local.getCmd().setErr(new PrintWriter(new OutputStreamWriter(err, UTF_8),
        true));

    int exitCode = local.execute(new String[] {"run", "--datanodes", "0"});

    assertEquals(-1, exitCode);
    assertTrue(err.toString(UTF_8.name()).contains("--datanodes"));
  }

  private static LocalOzoneClusterConfig resolve(String... args) {
    OzoneLocal.RunCommand command = new OzoneLocal.RunCommand();
    new CommandLine(command).parseArgs(args);
    return command.resolveConfig();
  }

  private static LocalOzoneClusterConfig resolveWithFallbackDefaults(
      String... args) {
    OzoneLocal.RunCommand command = new OzoneLocal.RunCommand();
    new CommandLine(command)
        .setDefaultValueProvider(new RunCommandFallbackDefaults())
        .parseArgs(args);
    return command.resolveConfig();
  }

  private static void assertConfigError(String option, String value,
      String expectedMessage) {
    OzoneLocal.RunCommand command = new OzoneLocal.RunCommand();
    new CommandLine(command).parseArgs(option, value);

    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        command::resolveConfig);

    assertTrue(error.getMessage().contains(expectedMessage),
        error.getMessage());
  }

  private static void assertParseError(String option,
      String expectedMessage) {
    OzoneLocal.RunCommand command = new OzoneLocal.RunCommand();
    ParameterException error = assertThrows(ParameterException.class,
        () -> new CommandLine(command).parseArgs(option));

    assertTrue(error.getMessage().contains(expectedMessage),
        error.getMessage());
  }

  private static void assertParseError(String option, String value,
      String expectedMessage) {
    OzoneLocal.RunCommand command = new OzoneLocal.RunCommand();
    ParameterException error = assertThrows(ParameterException.class,
        () -> new CommandLine(command).parseArgs(option, value));

    assertTrue(error.getMessage().contains(expectedMessage),
        error.getMessage());
  }

  private static void assertEnvDefault(String fieldName,
      String environmentVariable, String fallback) throws Exception {
    Field field = OzoneLocal.RunCommand.class.getDeclaredField(fieldName);
    String defaultValue = field.getAnnotation(Option.class).defaultValue();

    assertEquals("${env:" + environmentVariable + ":-" + fallback + "}",
        defaultValue);
  }

  private static final class TestableRunCommand extends OzoneLocal.RunCommand {

    private final LocalOzoneRuntime runtime;

    private TestableRunCommand(LocalOzoneRuntime runtime) {
      this.runtime = runtime;
    }

    @Override
    LocalOzoneRuntime createRuntime(LocalOzoneClusterConfig config, OzoneConfiguration seedConfiguration) {
      return runtime;
    }

    @Override
    void awaitShutdown(LocalOzoneRuntime localRuntime) {
      // Return immediately instead of blocking until JVM shutdown.
    }
  }

  private static final class StubRuntime implements LocalOzoneRuntime {

    private final String displayHost;
    private final int scmPort;
    private final int omPort;
    private final String s3Endpoint;
    private String reconEndpoint = "";
    private boolean failStart;
    private boolean started;
    private boolean closed;

    private StubRuntime(String displayHost, int scmPort, int omPort, String s3Endpoint) {
      this.displayHost = displayHost;
      this.scmPort = scmPort;
      this.omPort = omPort;
      this.s3Endpoint = s3Endpoint;
    }

    @Override
    public void start() {
      if (failStart) {
        throw new IllegalStateException("startup failed");
      }
      started = true;
    }

    @Override
    public String getDisplayHost() {
      return displayHost;
    }

    @Override
    public int getScmPort() {
      return scmPort;
    }

    @Override
    public int getOmPort() {
      return omPort;
    }

    @Override
    public int getS3gPort() {
      return 0;
    }

    @Override
    public String getS3Endpoint() {
      return s3Endpoint;
    }

    @Override
    public int getReconPort() {
      return 0;
    }

    @Override
    public String getReconEndpoint() {
      return reconEndpoint;
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  private static final class RunCommandFallbackDefaults
      implements IDefaultValueProvider {

    @Override
    public String defaultValue(ArgSpec argSpec) {
      if (!(argSpec instanceof OptionSpec)) {
        return null;
      }
      String option = ((OptionSpec) argSpec).longestName();
      if ("--data-dir".equals(option)) {
        return LocalOzoneClusterConfig.DEFAULT_DATA_DIR_VALUE;
      } else if ("--format".equals(option)) {
        return LocalOzoneClusterConfig.DEFAULT_FORMAT_MODE_VALUE;
      } else if ("--datanodes".equals(option)) {
        return LocalOzoneClusterConfig.DEFAULT_DATANODES_VALUE;
      } else if ("--host".equals(option)) {
        return LocalOzoneClusterConfig.DEFAULT_HOST;
      } else if ("--bind-host".equals(option)) {
        return LocalOzoneClusterConfig.DEFAULT_BIND_HOST;
      } else if ("--scm-port".equals(option)
          || "--om-port".equals(option)
          || "--s3g-port".equals(option)) {
        return LocalOzoneClusterConfig.DEFAULT_PORT_VALUE;
      } else if ("--recon-port".equals(option)) {
        return LocalOzoneClusterConfig.DEFAULT_PORT_VALUE;
      } else if ("--s3g".equals(option)) {
        return LocalOzoneClusterConfig.DEFAULT_S3G_ENABLED_VALUE;
      } else if ("--recon".equals(option)) {
        return LocalOzoneClusterConfig.DEFAULT_RECON_ENABLED_VALUE;
      } else if ("--ephemeral".equals(option)) {
        return LocalOzoneClusterConfig.DEFAULT_EPHEMERAL_VALUE;
      } else if ("--startup-timeout".equals(option)) {
        return LocalOzoneClusterConfig.DEFAULT_STARTUP_TIMEOUT_VALUE;
      }
      return null;
    }
  }
}
