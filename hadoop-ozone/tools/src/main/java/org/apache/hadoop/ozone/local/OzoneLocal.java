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

import java.nio.file.Path;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.TimeDurationUtil;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.Option;

/**
 * Internal CLI entry point for local Ozone cluster commands.
 */
@Command(name = "ozone local",
    hidden = true,
    description = "Internal commands for local Ozone cluster runtime",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    subcommands = {
        OzoneLocal.RunCommand.class
    })
public class OzoneLocal extends GenericCli {

  static final String ENV_DATA_DIR = "OZONE_LOCAL_DATA_DIR";
  static final String ENV_FORMAT = "OZONE_LOCAL_FORMAT";
  static final String ENV_DATANODES = "OZONE_LOCAL_DATANODES";
  static final String ENV_HOST = "OZONE_LOCAL_HOST";
  static final String ENV_BIND_HOST = "OZONE_LOCAL_BIND_HOST";
  static final String ENV_SCM_PORT = "OZONE_LOCAL_SCM_PORT";
  static final String ENV_OM_PORT = "OZONE_LOCAL_OM_PORT";
  static final String ENV_S3G_ENABLED = "OZONE_LOCAL_S3G_ENABLED";
  static final String ENV_S3G_PORT = "OZONE_LOCAL_S3G_PORT";
  static final String ENV_EPHEMERAL = "OZONE_LOCAL_EPHEMERAL";
  static final String ENV_STARTUP_TIMEOUT = "OZONE_LOCAL_STARTUP_TIMEOUT";
  static final String ENV_S3_ACCESS_KEY = "OZONE_LOCAL_S3_ACCESS_KEY";
  static final String ENV_S3_SECRET_KEY = "OZONE_LOCAL_S3_SECRET_KEY";
  static final String ENV_S3_REGION = "OZONE_LOCAL_S3_REGION";

  private static final String DEFAULT_DATA_DIR_VALUE = "${env:" + ENV_DATA_DIR
      + ":-" + LocalOzoneClusterConfig.DEFAULT_DATA_DIR_VALUE + "}";
  private static final String DEFAULT_FORMAT_VALUE = "${env:" + ENV_FORMAT
      + ":-" + LocalOzoneClusterConfig.DEFAULT_FORMAT_MODE_VALUE + "}";
  private static final String DEFAULT_DATANODES_VALUE = "${env:"
      + ENV_DATANODES + ":-" + LocalOzoneClusterConfig.DEFAULT_DATANODES_VALUE
      + "}";
  private static final String DEFAULT_HOST_VALUE = "${env:" + ENV_HOST
      + ":-" + LocalOzoneClusterConfig.DEFAULT_HOST + "}";
  private static final String DEFAULT_BIND_HOST_VALUE = "${env:"
      + ENV_BIND_HOST + ":-" + LocalOzoneClusterConfig.DEFAULT_BIND_HOST + "}";
  private static final String DEFAULT_SCM_PORT_VALUE = "${env:"
      + ENV_SCM_PORT + ":-" + LocalOzoneClusterConfig.DEFAULT_PORT_VALUE
      + "}";
  private static final String DEFAULT_OM_PORT_VALUE = "${env:" + ENV_OM_PORT
      + ":-" + LocalOzoneClusterConfig.DEFAULT_PORT_VALUE + "}";
  private static final String DEFAULT_S3G_ENABLED_VALUE = "${env:"
      + ENV_S3G_ENABLED + ":-"
      + LocalOzoneClusterConfig.DEFAULT_S3G_ENABLED_VALUE + "}";
  private static final String DEFAULT_S3G_PORT_VALUE = "${env:" + ENV_S3G_PORT
      + ":-" + LocalOzoneClusterConfig.DEFAULT_PORT_VALUE + "}";
  private static final String DEFAULT_EPHEMERAL_VALUE = "${env:"
      + ENV_EPHEMERAL + ":-"
      + LocalOzoneClusterConfig.DEFAULT_EPHEMERAL_VALUE + "}";
  private static final String DEFAULT_STARTUP_TIMEOUT_VALUE = "${env:"
      + ENV_STARTUP_TIMEOUT + ":-"
      + LocalOzoneClusterConfig.DEFAULT_STARTUP_TIMEOUT_VALUE + "}";
  private static final String DEFAULT_S3_ACCESS_KEY_VALUE = "${env:"
      + ENV_S3_ACCESS_KEY + ":-"
      + LocalOzoneClusterConfig.DEFAULT_S3_ACCESS_KEY + "}";
  private static final String DEFAULT_S3_SECRET_KEY_VALUE = "${env:"
      + ENV_S3_SECRET_KEY + ":-"
      + LocalOzoneClusterConfig.DEFAULT_S3_SECRET_KEY + "}";
  private static final String DEFAULT_S3_REGION_VALUE = "${env:"
      + ENV_S3_REGION + ":-" + LocalOzoneClusterConfig.DEFAULT_S3_REGION + "}";

  public OzoneLocal() {
    super();
  }

  OzoneLocal(CommandLine.IFactory factory) {
    super(factory);
  }

  public static void main(String[] args) {
    new OzoneLocal().run(args);
  }

  @Command(name = "run",
      hidden = true,
      description = "Resolve configuration for local Ozone runtime startup")
  static class RunCommand extends AbstractSubcommand implements Callable<Void> {

    @Option(names = "--data-dir",
        defaultValue = DEFAULT_DATA_DIR_VALUE,
        description = "Persistent data directory for the local cluster")
    private Path dataDir;

    @Option(names = "--format",
        converter = FormatModeConverter.class,
        defaultValue = DEFAULT_FORMAT_VALUE,
        description = "Storage init mode: if-needed, always, never")
    private LocalOzoneClusterConfig.FormatMode formatMode;

    @Option(names = "--datanodes",
        defaultValue = DEFAULT_DATANODES_VALUE,
        description = "Number of datanodes to start")
    private int datanodes;

    @Option(names = "--host",
        defaultValue = DEFAULT_HOST_VALUE,
        description = "Advertised host to write into local service addresses")
    private String host;

    @Option(names = "--bind-host",
        defaultValue = DEFAULT_BIND_HOST_VALUE,
        description = "Bind host for HTTP and RPC listeners")
    private String bindHost;

    @Option(names = "--scm-port",
        defaultValue = DEFAULT_SCM_PORT_VALUE,
        description = "SCM client RPC port (0 means auto-allocate)")
    private int scmPort;

    @Option(names = "--om-port",
        defaultValue = DEFAULT_OM_PORT_VALUE,
        description = "OM RPC port (0 means auto-allocate)")
    private int omPort;

    @Option(names = "--s3g-port",
        defaultValue = DEFAULT_S3G_PORT_VALUE,
        description = "S3 Gateway HTTP port (0 means auto-allocate)")
    private int s3gPort;

    @Option(names = "--s3g",
        negatable = true,
        defaultValue = DEFAULT_S3G_ENABLED_VALUE,
        fallbackValue = "true",
        description = "Enable S3 Gateway")
    private boolean s3gEnabled;

    @Option(names = "--ephemeral",
        negatable = true,
        defaultValue = DEFAULT_EPHEMERAL_VALUE,
        fallbackValue = "true",
        description = "Delete the data directory on shutdown")
    private boolean ephemeral;

    @Option(names = "--startup-timeout",
        converter = DurationConverter.class,
        defaultValue = DEFAULT_STARTUP_TIMEOUT_VALUE,
        description = "How long to wait for the local cluster to become ready")
    private Duration startupTimeout;

    @Option(names = "--s3-access-key",
        defaultValue = DEFAULT_S3_ACCESS_KEY_VALUE,
        description = "Suggested local AWS access key to print on startup")
    private String s3AccessKey;

    @Option(names = "--s3-secret-key",
        defaultValue = DEFAULT_S3_SECRET_KEY_VALUE,
        description = "Suggested local AWS secret key to print on startup")
    private String s3SecretKey;

    @Option(names = "--s3-region",
        defaultValue = DEFAULT_S3_REGION_VALUE,
        description = "Suggested local AWS region to print on startup")
    private String s3Region;

    @Override
    public Void call() {
      resolveConfig();
      return null;
    }

    LocalOzoneClusterConfig resolveConfig() {
      if (datanodes < 1) {
        throw new IllegalArgumentException(
            "Datanode count for --datanodes must be at least 1.");
      }
      validatePort(scmPort, "--scm-port");
      validatePort(omPort, "--om-port");
      validatePort(s3gPort, "--s3g-port");
      validateStartupTimeout();

      return LocalOzoneClusterConfig.builder(dataDir)
          .setFormatMode(formatMode)
          .setDatanodes(datanodes)
          .setHost(host)
          .setBindHost(bindHost)
          .setScmPort(scmPort)
          .setOmPort(omPort)
          .setS3gPort(s3gPort)
          .setS3gEnabled(s3gEnabled)
          .setEphemeral(ephemeral)
          .setStartupTimeout(startupTimeout)
          .setS3AccessKey(s3AccessKey)
          .setS3SecretKey(s3SecretKey)
          .setS3Region(s3Region)
          .build();
    }

    private void validateStartupTimeout() {
      if (startupTimeout.isZero() || startupTimeout.isNegative()) {
        throw new IllegalArgumentException(
            "Startup timeout for --startup-timeout must be greater than zero.");
      }
    }

    private static void validatePort(int value, String source) {
      if (value < 0 || value > 65_535) {
        throw new IllegalArgumentException("Port value for " + source
            + " must be between 0 and 65535.");
      }
    }

    private static final class FormatModeConverter
        implements ITypeConverter<LocalOzoneClusterConfig.FormatMode> {

      @Override
      public LocalOzoneClusterConfig.FormatMode convert(String value) {
        try {
          return LocalOzoneClusterConfig.FormatMode.fromString(value);
        } catch (IllegalArgumentException ex) {
          throw new CommandLine.TypeConversionException(
              "Expected one of: if-needed, always, never.");
        }
      }
    }

    private static final class DurationConverter
        implements ITypeConverter<Duration> {

      @Override
      public Duration convert(String value) {
        try {
          return Duration.parse(value.trim());
        } catch (DateTimeParseException ignored) {
          return parseHadoopStyleDuration(value);
        }
      }

      private static Duration parseHadoopStyleDuration(String value) {
        try {
          return TimeDurationUtil.getDuration("--startup-timeout", value,
              TimeUnit.MILLISECONDS);
        } catch (RuntimeException ex) {
          throw new CommandLine.TypeConversionException(durationMessage());
        }
      }

      private static String durationMessage() {
        return "Use ISO-8601 like PT2M or Hadoop-style values like 120s.";
      }
    }
  }
}
