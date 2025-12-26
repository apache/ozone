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

package org.apache.hadoop.ozone.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Objects;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.hadoop.hdds.server.YamlUtils;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * An abstract serializer for objects that extend the {@link WithChecksum} interface.
 * This class provides mechanisms for serializing and deserializing objects
 * in a YAML format.
 */
public abstract class YamlSerializer<T extends WithChecksum<T>> implements ObjectSerializer<T> {

  private static final Logger LOG = LoggerFactory.getLogger(YamlSerializer.class);

  private final GenericObjectPool<Yaml> yamlPool;

  public YamlSerializer(BasePooledObjectFactory<Yaml> yamlFactory) {
    this.yamlPool = new GenericObjectPool<>(yamlFactory);
  }

  private UncheckedAutoCloseableSupplier<Yaml> getYaml() throws IOException {
    try {
      Yaml yaml = yamlPool.borrowObject();
      return new UncheckedAutoCloseableSupplier<Yaml>() {

        @Override
        public void close() {
          yamlPool.returnObject(yaml);
        }

        @Override
        public Yaml get() {
          return yaml;
        }
      };
    } catch (Exception e) {
      throw new IOException("Failed to get yaml object.", e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public T load(File yamlFile) throws IOException {
    Objects.requireNonNull(yamlFile, "yamlFile cannot be null");
    try (InputStream inputFileStream = Files.newInputStream(yamlFile.toPath())) {
      return load(inputFileStream);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public T load(InputStream input) throws IOException {
    T dataYaml;
    try (UncheckedAutoCloseableSupplier<Yaml> yaml = getYaml()) {
      dataYaml = yaml.get().load(input);
    } catch (Exception e) {
      throw new IOException("Failed to load file", e);
    }

    if (dataYaml == null) {
      // If Yaml#load returned null, then the file is empty. This is valid yaml
      // but considered an error in this case since we have lost data about
      // the snapshot.
      throw new IOException("Failed to load file. File is empty.");
    }

    return dataYaml;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean verifyChecksum(T data) throws IOException {
    Objects.requireNonNull(data, "data cannot be null");

    // Get the stored checksum
    String storedChecksum = data.getChecksum();
    if (storedChecksum == null) {
      LOG.warn("No checksum found in snapshot data for verification");
      return false;
    }

    // Create a copy of the snapshot data for computing checksum
    T copy = data.copyObject();

    // Get the YAML representation
    try (UncheckedAutoCloseableSupplier<Yaml> yaml = getYaml()) {
      // Compute new checksum
      computeAndSetChecksum(yaml.get(), copy);

      // Compare the stored and computed checksums
      String computedChecksum = copy.getChecksum();
      boolean isValid = storedChecksum.equals(computedChecksum);

      if (!isValid) {
        LOG.warn("Checksum verification failed for snapshot local data. " +
            "Stored: {}, Computed: {}", storedChecksum, computedChecksum);
      }
      return isValid;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void save(File yamlFile, T data) throws IOException {
    // Create Yaml
    try (UncheckedAutoCloseableSupplier<Yaml> yaml = getYaml()) {
      // Compute Checksum and update SnapshotData
      computeAndSetChecksum(yaml.get(), data);
      // Write the object with checksum to Yaml file.
      YamlUtils.dump(yaml.get(), data, yamlFile, LOG);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    yamlPool.close();
  }

  public abstract void computeAndSetChecksum(Yaml yaml, T data) throws IOException;

}
