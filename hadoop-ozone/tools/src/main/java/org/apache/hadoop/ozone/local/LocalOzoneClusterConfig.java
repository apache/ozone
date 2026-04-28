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
import java.util.Locale;
import java.util.Objects;

/**
 * Configuration for a local single-node Ozone runtime.
 *
 * <p>This immutable configuration holds all settings needed to start
 * a local Ozone cluster including datanode count, network settings,
 * and storage options.</p>
 */
public final class LocalOzoneClusterConfig {

  /** Default advertised host for local services. */
  public static final String DEFAULT_HOST = "127.0.0.1";

  /** Default bind host for local services. */
  public static final String DEFAULT_BIND_HOST = "0.0.0.0";

  /** Default number of datanodes to start. */
  public static final int DEFAULT_DATANODES = 1;

  /** Default timeout waiting for cluster to become ready. */
  public static final Duration DEFAULT_STARTUP_TIMEOUT = Duration.ofMinutes(2);

  private final Path dataDir;
  private final FormatMode formatMode;
  private final int datanodes;
  private final boolean ephemeral;
  private final String host;
  private final String bindHost;
  private final Duration startupTimeout;

  private LocalOzoneClusterConfig(Builder builder) {
    this.dataDir = Objects.requireNonNull(builder.dataDir, "dataDir");
    this.formatMode = Objects.requireNonNull(builder.formatMode, "formatMode");
    this.datanodes = builder.datanodes;
    this.ephemeral = builder.ephemeral;
    this.host = Objects.requireNonNull(builder.host, "host");
    this.bindHost = Objects.requireNonNull(builder.bindHost, "bindHost");
    this.startupTimeout = Objects.requireNonNull(builder.startupTimeout,
        "startupTimeout");
  }

  /**
   * Returns the root data directory for the local cluster.
   */
  public Path getDataDir() {
    return dataDir;
  }

  /**
   * Returns the storage format mode.
   */
  public FormatMode getFormatMode() {
    return formatMode;
  }

  /**
   * Returns the number of datanodes to start.
   */
  public int getDatanodes() {
    return datanodes;
  }

  /**
   * Returns whether the data directory should be deleted on shutdown.
   */
  public boolean isEphemeral() {
    return ephemeral;
  }

  /**
   * Returns the advertised host for service addresses.
   */
  public String getHost() {
    return host;
  }

  /**
   * Returns the bind host for service listeners.
   */
  public String getBindHost() {
    return bindHost;
  }

  /**
   * Returns the timeout for waiting for cluster readiness.
   */
  public Duration getStartupTimeout() {
    return startupTimeout;
  }

  /**
   * Creates a new builder with the specified data directory.
   *
   * @param dataDir the root data directory for the local cluster
   * @return a new builder instance
   */
  public static Builder builder(Path dataDir) {
    return new Builder(dataDir);
  }

  /**
   * Storage initialization mode for the local runtime.
   */
  public enum FormatMode {
    /** Format storage only if not already initialized. */
    IF_NEEDED,
    /** Always format storage, destroying existing data. */
    ALWAYS,
    /** Never format; fail if storage is not initialized. */
    NEVER;

    /**
     * Parses a format mode from string representation.
     *
     * @param value the string value (e.g., "if-needed", "always", "never")
     * @return the corresponding FormatMode
     * @throws IllegalArgumentException if the value is not recognized
     */
    public static FormatMode fromString(String value) {
      return valueOf(value.trim().toUpperCase(Locale.ROOT).replace('-', '_'));
    }
  }

  /**
   * Builder for {@link LocalOzoneClusterConfig}.
   */
  public static final class Builder {

    private final Path dataDir;
    private FormatMode formatMode = FormatMode.IF_NEEDED;
    private int datanodes = DEFAULT_DATANODES;
    private boolean ephemeral;
    private String host = DEFAULT_HOST;
    private String bindHost = DEFAULT_BIND_HOST;
    private Duration startupTimeout = DEFAULT_STARTUP_TIMEOUT;

    private Builder(Path dataDir) {
      this.dataDir = dataDir.toAbsolutePath().normalize();
    }

    /**
     * Sets the storage format mode.
     */
    public Builder setFormatMode(FormatMode value) {
      this.formatMode = value;
      return this;
    }

    /**
     * Sets the number of datanodes to start.
     *
     * @param value the datanode count, must be at least 1
     * @throws IllegalArgumentException if value is less than 1
     */
    public Builder setDatanodes(int value) {
      if (value < 1) {
        throw new IllegalArgumentException(
            "Datanode count must be at least 1, got: " + value);
      }
      this.datanodes = value;
      return this;
    }

    /**
     * Sets whether the data directory should be deleted on shutdown.
     */
    public Builder setEphemeral(boolean value) {
      this.ephemeral = value;
      return this;
    }

    /**
     * Sets the advertised host for service addresses.
     */
    public Builder setHost(String value) {
      this.host = value;
      return this;
    }

    /**
     * Sets the bind host for service listeners.
     */
    public Builder setBindHost(String value) {
      this.bindHost = value;
      return this;
    }

    /**
     * Sets the timeout for waiting for cluster readiness.
     */
    public Builder setStartupTimeout(Duration value) {
      this.startupTimeout = value;
      return this;
    }

    /**
     * Builds the configuration.
     *
     * @return an immutable LocalOzoneClusterConfig instance
     */
    public LocalOzoneClusterConfig build() {
      return new LocalOzoneClusterConfig(this);
    }
  }
}
