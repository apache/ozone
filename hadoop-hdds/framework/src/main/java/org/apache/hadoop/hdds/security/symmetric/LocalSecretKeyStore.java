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

package org.apache.hadoop.hdds.security.symmetric;

import static com.google.common.collect.Sets.newHashSet;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.createFile;
import static java.nio.file.Files.exists;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SecretKeyStore} that saves and loads SecretKeys from/to a
 * JSON file on local file system.
 */
public class LocalSecretKeyStore implements SecretKeyStore {
  private static final Set<PosixFilePermission> SECRET_KEYS_PERMISSIONS =
      newHashSet(OWNER_READ, OWNER_WRITE);
  private static final Logger LOG =
      LoggerFactory.getLogger(LocalSecretKeyStore.class);

  private final Path secretKeysFile;
  private final ObjectMapper mapper;

  public LocalSecretKeyStore(Path secretKeysFile) {
    this.secretKeysFile = requireNonNull(secretKeysFile);
    this.mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
  }

  @Override
  public synchronized List<ManagedSecretKey> load() {
    if (!secretKeysFile.toFile().exists()) {
      return Collections.emptyList();
    }

    ObjectReader reader = mapper.readerFor(ManagedSecretKeyDto.class);
    try (MappingIterator<ManagedSecretKeyDto> iterator =
             reader.readValues(secretKeysFile.toFile())) {
      List<ManagedSecretKeyDto> dtos = iterator.readAll();
      List<ManagedSecretKey> result = dtos.stream()
          .map(ManagedSecretKeyDto::toObject)
          .collect(toList());
      LOG.info("Loaded {} from {}", result, secretKeysFile);
      return result;
    } catch (IOException e) {
      throw new IllegalStateException("Error reading SecretKeys from "
          + secretKeysFile, e);
    }
  }

  @Override
  public synchronized void save(Collection<ManagedSecretKey> secretKeys) {
    createSecretKeyFiles();

    List<ManagedSecretKeyDto> dtos = secretKeys.stream()
        .map(ManagedSecretKeyDto::new)
        .collect(toList());

    try (SequenceWriter writer =
             mapper.writer().writeValues(secretKeysFile.toFile())) {
      writer.init(true);
      writer.writeAll(dtos);
    } catch (IOException e) {
      throw new IllegalStateException("Error saving SecretKeys to file "
          + secretKeysFile, e);
    }
    LOG.info("Saved {} to file {}", secretKeys, secretKeysFile);
  }

  private void createSecretKeyFiles() {
    try {
      if (!exists(secretKeysFile)) {
        Path parent = secretKeysFile.getParent();
        if (parent != null && !exists(parent)) {
          createDirectories(parent);
        }
        createFile(secretKeysFile);
      }
      Files.setPosixFilePermissions(secretKeysFile, SECRET_KEYS_PERMISSIONS);
    } catch (IOException e) {
      throw new IllegalStateException("Error setting secret keys file" +
          " permission: " + secretKeysFile, e);
    }
  }

  /**
   * Just a simple DTO that allows serializing/deserializing the immutable
   * {@link ManagedSecretKey} objects.
   */
  private static class ManagedSecretKeyDto {
    private UUID id;
    private Instant creationTime;
    private Instant expiryTime;
    private String algorithm;
    private byte[] encoded;

    /**
     * Used by Jackson when deserializing.
     */
    ManagedSecretKeyDto() {
    }

    ManagedSecretKeyDto(ManagedSecretKey object) {
      id = object.getId();
      creationTime = object.getCreationTime();
      expiryTime = object.getExpiryTime();
      algorithm = object.getSecretKey().getAlgorithm();
      encoded = object.getSecretKey().getEncoded();
    }

    public ManagedSecretKey toObject() {
      SecretKey secretKey = new SecretKeySpec(this.encoded, this.algorithm);
      return new ManagedSecretKey(id, creationTime,
          expiryTime, secretKey);
    }

    public UUID getId() {
      return id;
    }

    public void setId(UUID id) {
      this.id = id;
    }

    public Instant getCreationTime() {
      return creationTime;
    }

    public void setCreationTime(Instant creationTime) {
      this.creationTime = creationTime;
    }

    public Instant getExpiryTime() {
      return expiryTime;
    }

    public void setExpiryTime(Instant expiryTime) {
      this.expiryTime = expiryTime;
    }

    public String getAlgorithm() {
      return algorithm;
    }

    public void setAlgorithm(String algorithm) {
      this.algorithm = algorithm;
    }

    public byte[] getEncoded() {
      return encoded;
    }

    public void setEncoded(byte[] encoded) {
      this.encoded = encoded;
    }
  }
}
