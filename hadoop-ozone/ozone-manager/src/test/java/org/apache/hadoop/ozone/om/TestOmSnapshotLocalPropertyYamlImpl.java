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

package org.apache.hadoop.ozone.om;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdds.server.YamlUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

/**
 * Tests for {@link OmSnapshotLocalPropertyYamlImpl}.
 */
public class TestOmSnapshotLocalPropertyYamlImpl {

  private static final Logger LOG = LoggerFactory.getLogger(TestOmSnapshotLocalPropertyYamlImpl.class);

  @TempDir
  private Path tempDir;

  private File yamlFile;
  private OmSnapshotLocalPropertyYamlImpl propertyImpl;

  @BeforeEach
  public void setUp() throws IOException {
    yamlFile = tempDir.resolve("test-properties.yaml").toFile();
    propertyImpl = new OmSnapshotLocalPropertyYamlImpl(yamlFile.getAbsolutePath());
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (propertyImpl != null) {
      propertyImpl.close();
    }

    if (yamlFile.exists()) {
      yamlFile.delete();
    }
  }

  @Test
  public void testInitWithNonExistentFile() throws IOException {
    // Verify that the implementation carries an empty properties map when the YAML file doesn't exist
    Map<String, String> properties = propertyImpl.getProperties();
    assertNotNull(properties);
    assertTrue(properties.isEmpty());

    // Now set a property and close to create the file
    String key = "testKey";
    String value = "testValue";
    propertyImpl.setProperty(key, value);

    // File should not be created until close is called
    assertFalse(yamlFile.exists(), "YAML file should not exist before close");

    propertyImpl.close();
    assertTrue(yamlFile.exists(), "YAML file should be created on close");
  }

  @Test
  public void testSetAndGetProperty() throws IOException {
    // Set a property
    String key = "testKey";
    String value = "testValue";
    propertyImpl.setProperty(key, value);

    // Verify the property was set in memory
    assertEquals(value, propertyImpl.getProperty(key));
    assertTrue(propertyImpl.hasProperty(key));

    // Verify the file is created with correct content after close
    propertyImpl.close();
    assertTrue(yamlFile.exists());

    // Read back for verification
    try (OmSnapshotLocalPropertyYamlImpl newPropertyImpl =
             new OmSnapshotLocalPropertyYamlImpl(yamlFile.getAbsolutePath())) {
      assertEquals(value, newPropertyImpl.getProperty(key));
    }
  }

  @Test
  public void testSetPropertyNoWriteUponCloseWhenNotDirty() throws IOException {
    // Setting the same property with the same value should not mark as dirty
    String key = "testKey";
    String value = "testValue";

    propertyImpl.setProperty(key, value);
    propertyImpl.close();

    // close() should create the file if it doesn't exist in this case
    assertTrue(yamlFile.exists());

    // Store the last modification time
    long lastModified = yamlFile.lastModified();

    propertyImpl = new OmSnapshotLocalPropertyYamlImpl(yamlFile.getAbsolutePath());
    // Same key-value, should not be dirty
    propertyImpl.setProperty(key, value);
    propertyImpl.close();

    // File modification time should not have changed since nothing changed
    assertEquals(lastModified, yamlFile.lastModified(),
        "File should not have been modified since properties didn't change");
  }

  @Test
  public void testUpdateProperty() throws IOException {
    // Set and then update a property
    String key = "testKey";
    String originalValue = "originalValue";
    String updatedValue = "updatedValue";

    propertyImpl.setProperty(key, originalValue);
    assertEquals(originalValue, propertyImpl.getProperty(key));

    propertyImpl.setProperty(key, updatedValue);
    assertEquals(updatedValue, propertyImpl.getProperty(key));

    propertyImpl.close();

    // Verify updated value is persisted
    try (OmSnapshotLocalPropertyYamlImpl newPropertyImpl =
        new OmSnapshotLocalPropertyYamlImpl(yamlFile.getAbsolutePath())) {
      assertEquals(updatedValue, newPropertyImpl.getProperty(key));
    }
  }

  @Test
  public void testRemoveProperty() throws IOException {
    // Set and then remove a property
    String key = "testKey";
    String value = "testValue";

    propertyImpl.setProperty(key, value);
    assertTrue(propertyImpl.hasProperty(key));

    propertyImpl.removeProperty(key);
    assertFalse(propertyImpl.hasProperty(key));
    assertNull(propertyImpl.getProperty(key));

    propertyImpl.close();

    try (OmSnapshotLocalPropertyYamlImpl newPropertyImpl =
        new OmSnapshotLocalPropertyYamlImpl(yamlFile.getAbsolutePath())) {
      assertFalse(newPropertyImpl.hasProperty(key));
    }
  }

  @Test
  public void testRemoveNonExistentProperty() throws IOException {
    // Removing a non-existent property should not cause errors
    propertyImpl.removeProperty("nonExistentKey");

    // Should not mark as dirty, so file shouldn't be created on close
    propertyImpl.close();
    assertFalse(yamlFile.exists());
  }

  @Test
  public void testGetProperties() throws IOException {
    // Set multiple properties
    Map<String, String> testProperties = new HashMap<>();
    testProperties.put("key1", "value1");
    testProperties.put("key2", "value2");
    testProperties.put("key3", "value3");

    for (Map.Entry<String, String> entry : testProperties.entrySet()) {
      propertyImpl.setProperty(entry.getKey(), entry.getValue());
    }

    // Verify all properties set
    Map<String, String> retrievedProperties = propertyImpl.getProperties();
    assertEquals(testProperties.size(), retrievedProperties.size());
    for (Map.Entry<String, String> entry : testProperties.entrySet()) {
      assertEquals(entry.getValue(), retrievedProperties.get(entry.getKey()));
    }
  }

  @Test
  public void testLoadFromExistingFile() throws IOException {
    // Create a file with predefined properties
    Map<String, String> initialProperties = new HashMap<>();
    initialProperties.put("existing1", "value1");
    initialProperties.put("existing2", "value2");

    DumperOptions options = new DumperOptions();
    options.setPrettyFlow(true);
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.FLOW);
    Yaml yaml = new Yaml(options);

    YamlUtils.dump(yaml, initialProperties, yamlFile, LOG);

    // Instantiate with existing file
    try (OmSnapshotLocalPropertyYamlImpl loadedPropertyImpl =
        new OmSnapshotLocalPropertyYamlImpl(yamlFile.getAbsolutePath())) {
      // Verify properties are loaded correctly
      assertEquals("value1", loadedPropertyImpl.getProperty("existing1"));
      assertEquals("value2", loadedPropertyImpl.getProperty("existing2"));
      assertEquals(2, loadedPropertyImpl.getProperties().size());
    }
  }

  @Test
  public void testOperationsAfterClose() throws IOException {
    propertyImpl.close();

    // All operations should throw IOException after close
    assertThrows(IOException.class, () -> propertyImpl.getProperty("anyKey"));
    assertThrows(IOException.class, () -> propertyImpl.setProperty("anyKey", "anyValue"));
    assertThrows(IOException.class, () -> propertyImpl.hasProperty("anyKey"));
    assertThrows(IOException.class, () -> propertyImpl.removeProperty("anyKey"));
    assertThrows(IOException.class, () -> propertyImpl.getProperties());
  }

  @Test
  public void testInvalidYamlFile() throws IOException {
    // Create file with invalid YAML content
    Files.write(yamlFile.toPath(), "invalid: yaml: content: - not properly formatted".getBytes());

    // Should throw IOException with appropriate message
    IOException exception = assertThrows(IOException.class,
        () -> new OmSnapshotLocalPropertyYamlImpl(yamlFile.getAbsolutePath()));

    assertTrue(exception.getMessage().contains("Unable to parse snapshot local properties YAML file"));
  }
}
