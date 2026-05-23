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
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;

/**
 * Configuration for a local Ozone cluster runtime.
 *
 * <p>The datanode count describes how many local datanode services should run
 * on the same host.
 */
public final class LocalOzoneClusterConfig {

  private static final String DEFAULT_DATA_DIR_PARENT = ".ozone";
  private static final String DEFAULT_DATA_DIR_NAME = "local";

  // Picocli annotation defaults require compile-time strings, so the CLI uses
  // this expression while the typed default below uses the same path fragments.
  static final String DEFAULT_DATA_DIR_VALUE =
      "${sys:user.home}${sys:file.separator}" + DEFAULT_DATA_DIR_PARENT
          + "${sys:file.separator}" + DEFAULT_DATA_DIR_NAME;
  static final String DEFAULT_FORMAT_MODE_VALUE = "if-needed";
  static final String DEFAULT_DATANODES_VALUE = "1";
  static final String DEFAULT_PORT_VALUE = "0";
  static final String DEFAULT_S3G_ENABLED_VALUE = "true";
  static final String DEFAULT_EPHEMERAL_VALUE = "false";
  static final String DEFAULT_STARTUP_TIMEOUT_VALUE = "PT2M";

  static final Path DEFAULT_DATA_DIR =
      Paths.get(System.getProperty("user.home"), DEFAULT_DATA_DIR_PARENT,
          DEFAULT_DATA_DIR_NAME)
          .toAbsolutePath()
          .normalize();
  static final FormatMode DEFAULT_FORMAT_MODE =
      FormatMode.fromString(DEFAULT_FORMAT_MODE_VALUE);
  static final int DEFAULT_DATANODES =
      Integer.parseInt(DEFAULT_DATANODES_VALUE);
  static final String DEFAULT_HOST = "127.0.0.1";
  static final String DEFAULT_BIND_HOST = "0.0.0.0";
  static final int DEFAULT_PORT = Integer.parseInt(DEFAULT_PORT_VALUE);
  static final boolean DEFAULT_S3G_ENABLED =
      Boolean.parseBoolean(DEFAULT_S3G_ENABLED_VALUE);
  static final boolean DEFAULT_EPHEMERAL =
      Boolean.parseBoolean(DEFAULT_EPHEMERAL_VALUE);
  static final Duration DEFAULT_STARTUP_TIMEOUT =
      Duration.parse(DEFAULT_STARTUP_TIMEOUT_VALUE);
  static final String DEFAULT_S3_ACCESS_KEY = "admin";
  static final String DEFAULT_S3_SECRET_KEY = "admin123";
  static final String DEFAULT_S3_REGION = "us-east-1";

  private final Path dataDir;
  private final FormatMode formatMode;
  private final int datanodes;
  private final String host;
  private final String bindHost;
  private final int scmPort;
  private final int omPort;
  private final int s3gPort;
  private final boolean s3gEnabled;
  private final boolean ephemeral;
  private final Duration startupTimeout;
  private final String s3AccessKey;
  private final String s3SecretKey;
  private final String s3Region;

  private LocalOzoneClusterConfig(Builder builder) {
    dataDir = Objects.requireNonNull(builder.dataDir, "dataDir")
        .toAbsolutePath()
        .normalize();
    formatMode = Objects.requireNonNull(builder.formatMode, "formatMode");
    datanodes = builder.datanodes;
    host = Objects.requireNonNull(builder.host, "host");
    bindHost = Objects.requireNonNull(builder.bindHost, "bindHost");
    scmPort = builder.scmPort;
    omPort = builder.omPort;
    s3gPort = builder.s3gPort;
    s3gEnabled = builder.s3gEnabled;
    ephemeral = builder.ephemeral;
    startupTimeout = Objects.requireNonNull(builder.startupTimeout,
        "startupTimeout");
    s3AccessKey = Objects.requireNonNull(builder.s3AccessKey, "s3AccessKey");
    s3SecretKey = Objects.requireNonNull(builder.s3SecretKey, "s3SecretKey");
    s3Region = Objects.requireNonNull(builder.s3Region, "s3Region");
  }

  public Path getDataDir() {
    return dataDir;
  }

  public FormatMode getFormatMode() {
    return formatMode;
  }

  public int getDatanodes() {
    return datanodes;
  }

  public String getHost() {
    return host;
  }

  public String getBindHost() {
    return bindHost;
  }

  /**
   * Returns the SCM client RPC port. Port {@code 0} asks the runtime to choose
   * an available local port.
   */
  public int getScmPort() {
    return scmPort;
  }

  /**
   * Returns the OM RPC port. Port {@code 0} asks the runtime to choose
   * an available local port.
   */
  public int getOmPort() {
    return omPort;
  }

  /**
   * Returns the S3 Gateway HTTP port. Port {@code 0} asks the runtime to
   * choose an available local port.
   */
  public int getS3gPort() {
    return s3gPort;
  }

  /**
   * Returns whether the local runtime should include S3 Gateway.
   */
  public boolean isS3gEnabled() {
    return s3gEnabled;
  }

  /**
   * Returns whether the local runtime should remove its data directory when it
   * shuts down.
   */
  public boolean isEphemeral() {
    return ephemeral;
  }

  /**
   * Returns how long the launcher should wait for local services to become
   * ready before failing startup.
   */
  public Duration getStartupTimeout() {
    return startupTimeout;
  }

  /**
   * Returns the suggested local-only S3 access key printed for client setup.
   */
  public String getS3AccessKey() {
    return s3AccessKey;
  }

  /**
   * Returns the suggested local-only S3 secret key printed for client setup.
   */
  public String getS3SecretKey() {
    return s3SecretKey;
  }

  /**
   * Returns the suggested local-only S3 region printed for client setup.
   */
  public String getS3Region() {
    return s3Region;
  }

  public static Builder builder() {
    return new Builder(DEFAULT_DATA_DIR);
  }

  public static Builder builder(Path dataDir) {
    return new Builder(dataDir);
  }

  /**
   * Storage initialization mode for the local runtime.
   */
  public enum FormatMode {
    /**
     * Initialize storage only when local metadata is missing or unformatted.
     * Existing local data is reused.
     */
    IF_NEEDED,

    /**
     * Always format local storage before startup. Existing local data may be
     * discarded.
     */
    ALWAYS,

    /**
     * Never format local storage. Startup should fail later if required storage
     * is not already initialized.
     */
    NEVER;

    public static FormatMode fromString(String value) {
      if (value == null) {
        throw new IllegalArgumentException("Format mode must not be null.");
      }
      String normalized = value.trim().toUpperCase(Locale.ROOT)
          .replace('-', '_');
      return valueOf(normalized);
    }
  }

  /**
   * Builder for {@link LocalOzoneClusterConfig}.
   */
  public static final class Builder {

    private final Path dataDir;
    private FormatMode formatMode = DEFAULT_FORMAT_MODE;
    private int datanodes = DEFAULT_DATANODES;
    private String host = DEFAULT_HOST;
    private String bindHost = DEFAULT_BIND_HOST;
    private int scmPort = DEFAULT_PORT;
    private int omPort = DEFAULT_PORT;
    private int s3gPort = DEFAULT_PORT;
    private boolean s3gEnabled = DEFAULT_S3G_ENABLED;
    private boolean ephemeral = DEFAULT_EPHEMERAL;
    private Duration startupTimeout = DEFAULT_STARTUP_TIMEOUT;
    private String s3AccessKey = DEFAULT_S3_ACCESS_KEY;
    private String s3SecretKey = DEFAULT_S3_SECRET_KEY;
    private String s3Region = DEFAULT_S3_REGION;

    private Builder(Path dataDir) {
      this.dataDir = dataDir;
    }

    public Builder setFormatMode(FormatMode value) {
      formatMode = value;
      return this;
    }

    public Builder setDatanodes(int value) {
      datanodes = value;
      return this;
    }

    public Builder setHost(String value) {
      host = value;
      return this;
    }

    public Builder setBindHost(String value) {
      bindHost = value;
      return this;
    }

    public Builder setScmPort(int value) {
      scmPort = value;
      return this;
    }

    public Builder setOmPort(int value) {
      omPort = value;
      return this;
    }

    public Builder setS3gPort(int value) {
      s3gPort = value;
      return this;
    }

    public Builder setS3gEnabled(boolean value) {
      s3gEnabled = value;
      return this;
    }

    public Builder setEphemeral(boolean value) {
      ephemeral = value;
      return this;
    }

    public Builder setStartupTimeout(Duration value) {
      startupTimeout = value;
      return this;
    }

    public Builder setS3AccessKey(String value) {
      s3AccessKey = value;
      return this;
    }

    public Builder setS3SecretKey(String value) {
      s3SecretKey = value;
      return this;
    }

    public Builder setS3Region(String value) {
      s3Region = value;
      return this;
    }

    public LocalOzoneClusterConfig build() {
      return new LocalOzoneClusterConfig(this);
    }
  }
}
