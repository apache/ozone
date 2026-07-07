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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.time.Duration;
import org.junit.jupiter.api.Test;
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
  void localCommandMetadataIsPresentAndHidden() {
    Command command = OzoneLocal.class.getAnnotation(Command.class);

    assertNotNull(command);
    assertEquals("ozone local", command.name());
    assertTrue(command.hidden());
  }

  @Test
  void runCommandMetadataIsPresentAndHidden() {
    Command command = OzoneLocal.RunCommand.class.getAnnotation(Command.class);

    assertNotNull(command);
    assertEquals("run", command.name());
    assertTrue(command.hidden());
  }

  @Test
  void genericCliRegistersRunCommand() {
    OzoneLocal local = new OzoneLocal();

    assertTrue(local.getCmd().getSubcommands().containsKey("run"));
  }

  @Test
  void rootHelpHidesRunCommand() throws Exception {
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
    assertFalse(help.matches("(?s).*\\R\\s+run\\b.*"), help);
    assertEquals("", err.toString(UTF_8.name()));
  }

  @Test
  void runCommandResolvesConfigurationQuietlyUntilRuntimeStartup()
      throws Exception {
    OzoneLocal local = new OzoneLocal();
    CommandLine commandLine = local.getCmd();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    commandLine.setOut(new PrintWriter(new OutputStreamWriter(out, UTF_8),
        true));
    commandLine.setErr(new PrintWriter(new OutputStreamWriter(err, UTF_8),
        true));

    int exitCode = local.execute(new String[] {"run"});

    assertEquals(0, exitCode);
    assertEquals("", out.toString(UTF_8.name()));
    assertEquals("", err.toString(UTF_8.name()));
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
    assertEnvDefault("ephemeral", OzoneLocal.ENV_EPHEMERAL,
        LocalOzoneClusterConfig.DEFAULT_EPHEMERAL_VALUE);
    assertEnvDefault("startupTimeout", OzoneLocal.ENV_STARTUP_TIMEOUT,
        LocalOzoneClusterConfig.DEFAULT_STARTUP_TIMEOUT_VALUE);
    assertEnvDefault("s3AccessKey", OzoneLocal.ENV_S3_ACCESS_KEY,
        LocalOzoneClusterConfig.DEFAULT_S3_ACCESS_KEY);
    assertEnvDefault("s3SecretKey", OzoneLocal.ENV_S3_SECRET_KEY,
        LocalOzoneClusterConfig.DEFAULT_S3_SECRET_KEY);
    assertEnvDefault("s3Region", OzoneLocal.ENV_S3_REGION,
        LocalOzoneClusterConfig.DEFAULT_S3_REGION);
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
    assertEquals("0.0.0.0", config.getBindHost());
    assertEquals(0, config.getScmPort());
    assertEquals(0, config.getOmPort());
    assertEquals(0, config.getS3gPort());
    assertTrue(config.isS3gEnabled());
    assertFalse(config.isEphemeral());
    assertEquals(Duration.ofMinutes(2), config.getStartupTimeout());
    assertEquals("admin", config.getS3AccessKey());
    assertEquals("admin123", config.getS3SecretKey());
    assertEquals("us-east-1", config.getS3Region());
  }

  @Test
  void resolveConfigUsesCliOverrides() {
    LocalOzoneClusterConfig config = resolve(
        "--data-dir", "target/cli-local",
        "--format", "always",
        "--datanodes", "3",
        "--host", "cli-host",
        "--bind-host", "127.0.0.1",
        "--scm-port", "200",
        "--om-port", "201",
        "--s3g-port", "202",
        "--no-s3g",
        "--ephemeral",
        "--startup-timeout", "45s",
        "--s3-access-key", "cli-access",
        "--s3-secret-key", "cli-secret",
        "--s3-region", "cli-region");

    assertEquals(Paths.get("target/cli-local").toAbsolutePath().normalize(),
        config.getDataDir());
    assertEquals(LocalOzoneClusterConfig.FormatMode.ALWAYS,
        config.getFormatMode());
    assertEquals(3, config.getDatanodes());
    assertEquals("cli-host", config.getHost());
    assertEquals("127.0.0.1", config.getBindHost());
    assertEquals(200, config.getScmPort());
    assertEquals(201, config.getOmPort());
    assertEquals(202, config.getS3gPort());
    assertFalse(config.isS3gEnabled());
    assertTrue(config.isEphemeral());
    assertEquals(Duration.ofSeconds(45), config.getStartupTimeout());
    assertEquals("cli-access", config.getS3AccessKey());
    assertEquals("cli-secret", config.getS3SecretKey());
    assertEquals("cli-region", config.getS3Region());
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
  void resolveConfigRejectsInvalidFormat() {
    assertParseError("--format", "sometimes", "--format");
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

  @Test
  void resolveConfigRejectsInvalidDuration() {
    assertParseError("--startup-timeout", "forever", "--startup-timeout");
  }

  @Test
  void resolveConfigRejectsNonPositiveDuration() {
    assertConfigError("--startup-timeout", "0s", "--startup-timeout");
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
      } else if ("--s3g".equals(option)) {
        return LocalOzoneClusterConfig.DEFAULT_S3G_ENABLED_VALUE;
      } else if ("--ephemeral".equals(option)) {
        return LocalOzoneClusterConfig.DEFAULT_EPHEMERAL_VALUE;
      } else if ("--startup-timeout".equals(option)) {
        return LocalOzoneClusterConfig.DEFAULT_STARTUP_TIMEOUT_VALUE;
      } else if ("--s3-access-key".equals(option)) {
        return LocalOzoneClusterConfig.DEFAULT_S3_ACCESS_KEY;
      } else if ("--s3-secret-key".equals(option)) {
        return LocalOzoneClusterConfig.DEFAULT_S3_SECRET_KEY;
      } else if ("--s3-region".equals(option)) {
        return LocalOzoneClusterConfig.DEFAULT_S3_REGION;
      }
      return null;
    }
  }
}
