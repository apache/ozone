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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link PersistedPorts}.
 */
class TestPersistedPorts {

  @TempDir
  private Path tempDir;

  @Test
  void loadFromNonExistentFileReturnsEmpty() throws IOException {
    Path portsFile = tempDir.resolve("ports.properties");
    PersistedPorts ports = PersistedPorts.load(portsFile);

    assertEquals(0, ports.get("nonexistent.key"));
  }

  @Test
  void setAndGetReturnsValue() throws IOException {
    Path portsFile = tempDir.resolve("ports.properties");
    PersistedPorts ports = PersistedPorts.load(portsFile);

    ports.set("dn.0.http", 9878);
    assertEquals(9878, ports.get("dn.0.http"));
  }

  @Test
  void storeAndLoadPreservesValues() throws IOException {
    Path portsFile = tempDir.resolve("ports.properties");

    // Store some ports
    PersistedPorts ports1 = PersistedPorts.load(portsFile);
    ports1.set("dn.0.http", 9878);
    ports1.set("dn.0.client", 9879);
    ports1.set("dn.1.http", 9880);
    ports1.store();

    assertTrue(Files.exists(portsFile), "Ports file should be created");

    // Load and verify
    PersistedPorts ports2 = PersistedPorts.load(portsFile);
    assertEquals(9878, ports2.get("dn.0.http"));
    assertEquals(9879, ports2.get("dn.0.client"));
    assertEquals(9880, ports2.get("dn.1.http"));
    assertEquals(0, ports2.get("nonexistent"));
  }

  @Test
  void storeOverwritesExistingFile() throws IOException {
    Path portsFile = tempDir.resolve("ports.properties");

    // First store
    PersistedPorts ports1 = PersistedPorts.load(portsFile);
    ports1.set("key1", 1111);
    ports1.store();

    // Second store with different value
    PersistedPorts ports2 = PersistedPorts.load(portsFile);
    ports2.set("key1", 2222);
    ports2.set("key2", 3333);
    ports2.store();

    // Verify final state
    PersistedPorts ports3 = PersistedPorts.load(portsFile);
    assertEquals(2222, ports3.get("key1"));
    assertEquals(3333, ports3.get("key2"));
  }

  @Test
  void loadPreservesExistingValuesWhenAddingNew() throws IOException {
    Path portsFile = tempDir.resolve("ports.properties");

    // Store initial value
    PersistedPorts ports1 = PersistedPorts.load(portsFile);
    ports1.set("existing", 1111);
    ports1.store();

    // Load, add new, store
    PersistedPorts ports2 = PersistedPorts.load(portsFile);
    assertEquals(1111, ports2.get("existing"));
    ports2.set("new", 2222);
    ports2.store();

    // Verify both exist
    PersistedPorts ports3 = PersistedPorts.load(portsFile);
    assertEquals(1111, ports3.get("existing"));
    assertEquals(2222, ports3.get("new"));
  }
}
