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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.server.YamlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.scanner.ScannerException;

/**
 * Implementation of {@link OmSnapshotLocalProperty} that uses a YAML file
 * to store and retrieve snapshot local properties.
 * Changes are only persisted when the object is closed.
 */
public class OmSnapshotLocalPropertyYamlImpl implements OmSnapshotLocalProperty, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(OmSnapshotLocalPropertyYamlImpl.class);

  /**
   * The path to the YAML file used to store properties.
   * If this file does not exist, it will be created upon close().
   */
  private final File yamlFile;

  /**
   * A map storing key-value pairs of snapshot properties.
   * Read from the YAML file upon initialization.
   */
  private Map<String, String> properties;

  /**
   * Flag indicating whether the properties have been modified
   * since the last save to the YAML file.
   * Used to determine if file write is needed on close.
   */
  private final AtomicBoolean isDirty = new AtomicBoolean(false);

  /**
   * Flag indicating whether this instance has been closed.
   * Operations attempted after closing will throw an OMException.
   */
  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  /**
   * Constructs a new OmSnapshotLocalPropertyYamlImpl.
   *
   * @param yamlFilePath Path to the YAML file
   */
  public OmSnapshotLocalPropertyYamlImpl(String yamlFilePath) throws IOException {
    this.yamlFile = new File(yamlFilePath);
    loadPropertiesFromFile();
  }

  @Override
  public void setProperty(String key, String value) throws IOException {
    checkIfClosed();
    String oldValue = properties.get(key);
    if (!Objects.equals(value, oldValue)) {
      properties.put(key, value);
      isDirty.set(true);
    }
  }

  @Override
  public String getProperty(String key) throws IOException {
    checkIfClosed();
    return properties.get(key);
  }

  @Override
  public boolean hasProperty(String key) throws IOException {
    checkIfClosed();
    return properties.containsKey(key);
  }

  @Override
  public void removeProperty(String key) throws IOException {
    checkIfClosed();
    if (properties.containsKey(key)) {
      properties.remove(key);
      isDirty.set(true);
    }
  }

  @Override
  public Map<String, String> getProperties() throws IOException {
    checkIfClosed();
    return new HashMap<>(properties);
  }

  /**
   * Saves any pending changes to the YAML file and releases resources.
   *
   * @throws IOException if an I/O error occurs saving the file
   */
  @Override
  public void close() throws IOException {
    if (isClosed.compareAndSet(false, true)) {
      if (isDirty.get()) {
        LOG.debug("Saving changes to properties file: {}", yamlFile);
        savePropertiesToFile();
      }
    }
  }

  /**
   * Checks if the object has been closed.
   *
   * @throws IOException if the object has been closed
   */
  private void checkIfClosed() throws IOException {
    if (isClosed.get()) {
      throw new IOException("OmSnapshotLocalPropertyYamlImpl has been closed");
    }
  }

  /**
   * Loads the properties from the YAML file.
   *
   * @throws IOException if an I/O error occurs, or if the YAML file is not properly formatted
   */
  private void loadPropertiesFromFile() throws IOException {
    if (!yamlFile.exists()) {
      LOG.debug("YAML file does not exist, creating empty properties map");
      properties = new HashMap<>();
      return;
    }

    try (InputStream inputStream = Files.newInputStream(yamlFile.toPath())) {
      Map<String, String> loadedProperties = YamlUtils.loadAs(inputStream, Map.class);
      properties = loadedProperties != null ? new HashMap<>(loadedProperties) : new HashMap<>();
    } catch (IOException | ScannerException e) {
      LOG.error("Unable to parse YAML file: {}", yamlFile, e);
      throw new IOException("Unable to parse snapshot properties YAML file", e);
    }
  }

  /**
   * Saves the properties to the YAML file.
   *
   * @throws IOException if an I/O error occurs
   */
  private void savePropertiesToFile() throws IOException {
    DumperOptions options = new DumperOptions();
    options.setPrettyFlow(true);
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.FLOW);
    Yaml yaml = new Yaml(options);

    YamlUtils.dump(yaml, properties, yamlFile, LOG);
    isDirty.set(false);
  }
}
