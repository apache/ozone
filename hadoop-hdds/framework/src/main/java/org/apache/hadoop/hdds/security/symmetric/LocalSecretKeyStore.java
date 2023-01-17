/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.security.symmetric;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.google.common.collect.Sets.newHashSet;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static java.util.stream.Collectors.toList;

/**
 * A {@link SecretKeyStore} that saves and loads SecretKeys from/to a
 * JSON file on local file system.
 */
public class LocalSecretKeyStore implements SecretKeyStore {
  private static final Logger LOG = LoggerFactory.getLogger(LocalSecretKeyStore.class);

  private final Path secretKeysFile;
  private final ObjectMapper mapper;

  public LocalSecretKeyStore(Path secretKeysFile) {
    this.secretKeysFile = secretKeysFile;
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
    setFileOwnerPermissions(secretKeysFile);

    List<ManagedSecretKeyDto> dtos = secretKeys.stream()
        .map(ManagedSecretKeyDto::new)
        .collect(toList());

    try (SequenceWriter writer =
             mapper.writer().writeValues(secretKeysFile.toFile())) {
      writer.init(true);
      for (ManagedSecretKeyDto dto : dtos) {
        writer.write(dto);
      }
    } catch (IOException e) {
      throw new IllegalStateException("Error saving SecretKeys to file "
          + secretKeysFile, e);
    }
    LOG.info("Saved {} to file {}", secretKeys, secretKeysFile);
  }

  private void setFileOwnerPermissions(Path path) {
    Set<PosixFilePermission> permissions = newHashSet(OWNER_READ, OWNER_WRITE);
    try {
      if (!Files.exists(path)) {
        if (!Files.exists(path.getParent())) {
          Files.createDirectories(path.getParent());
        }
        Files.createFile(path);
      }
      Files.setPosixFilePermissions(path, permissions);
    } catch (IOException e) {
      throw new IllegalStateException("Error setting secret keys file" +
          " permission: " + secretKeysFile, e);
    }
  }

  private static class ManagedSecretKeyDto {
    private UUID id;
    private Instant creationTime;
    private Instant expiryTime;
    private SecretKey secretKey;

    /**
     * Used by Jackson when deserializing.
     */
    ManagedSecretKeyDto() {
    }

    ManagedSecretKeyDto(ManagedSecretKey object) {
      id = object.getId();
      creationTime = object.getCreationTime();
      expiryTime = object.getExpiryTime();
      secretKey = object.getSecretKey();
    }

    public ManagedSecretKey toObject() {
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

    @JsonSerialize(using = SecretKeySerializer.class)
    @JsonDeserialize(using = SecretKeyDeserializer.class)
    public SecretKey getSecretKey() {
      return secretKey;
    }

    public void setSecretKey(SecretKey secretKey) {
      this.secretKey = secretKey;
    }
  }

  private static class SecretKeySerializer extends StdSerializer<SecretKey> {
    SecretKeySerializer() {
      super(SecretKey.class);
    }

    @Override
    public void serialize(SecretKey value, JsonGenerator gen,
                          SerializerProvider provider) throws IOException {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(bos);
      oos.writeObject(value);
      gen.writeBinary(bos.toByteArray());
    }
  }

  private static class SecretKeyDeserializer extends
      StdDeserializer<SecretKey> {
    SecretKeyDeserializer() {
      super(SecretKey.class);
    }

    @Override
    public SecretKey deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      ByteArrayInputStream bis = new ByteArrayInputStream(p.getBinaryValue());
      ObjectInputStream ois = new ObjectInputStream(bis);
      try {
        return (SecretKey) ois.readObject();
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException("Error reading SecretKey", e);
      }
    }
  }
}
