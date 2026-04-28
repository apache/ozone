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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Persists allocated ports to enable stable endpoints across restarts.
 *
 * <p>When a local Ozone cluster starts, it allocates ephemeral ports for
 * its services. This class saves those ports to a properties file so that
 * subsequent restarts can reuse the same ports, providing stable endpoints
 * for clients.</p>
 */
final class PersistedPorts {

  private final Path path;
  private final Properties properties = new Properties();

  private PersistedPorts(Path path) {
    this.path = path;
  }

  /**
   * Loads persisted ports from the specified file.
   *
   * @param path the path to the ports properties file
   * @return a PersistedPorts instance, empty if file doesn't exist
   * @throws IOException if reading the file fails
   */
  static PersistedPorts load(Path path) throws IOException {
    PersistedPorts persistedPorts = new PersistedPorts(path);
    if (Files.exists(path)) {
      try (InputStream input = Files.newInputStream(path)) {
        persistedPorts.properties.load(input);
      }
    }
    return persistedPorts;
  }

  /**
   * Gets a previously persisted port value.
   *
   * @param key the port identifier (e.g., "dn.0.container.ipc")
   * @return the port number, or 0 if not persisted
   */
  int get(String key) {
    String value = properties.getProperty(key);
    return value == null ? 0 : Integer.parseInt(value);
  }

  /**
   * Sets a port value to be persisted.
   *
   * @param key the port identifier
   * @param port the port number
   */
  void set(String key, int port) {
    properties.setProperty(key, Integer.toString(port));
  }

  /**
   * Saves all port values to the file.
   *
   * @throws IOException if writing the file fails
   */
  void store() throws IOException {
    try (OutputStream output = Files.newOutputStream(path)) {
      properties.store(output, "Local Ozone reserved ports");
    }
  }
}
