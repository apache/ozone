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

package org.apache.hadoop.ozone.client;

import static org.apache.hadoop.ozone.OzoneConsts.ETAG;
import static org.apache.hadoop.ozone.OzoneConsts.MD5_HASH;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.storage.ByteBufferStreamOutput;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts.PartInfo;
import org.apache.hadoop.ozone.client.io.KeyDataStreamOutput;
import org.apache.hadoop.ozone.client.io.KeyMetadataAware;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.ErrorInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.ratis.util.function.CheckedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory ozone bucket for testing.
 */
public final class OzoneBucketStub extends OzoneBucket {

  private static final Logger LOG = LoggerFactory.getLogger(OzoneBucketStub.class);

  private Map<String, OzoneKeyDetails> keyDetails = new HashMap<>();

  private Map<String, byte[]> keyContents = new HashMap<>();

  private Map<String, MultipartInfoStub> keyToMultipartUpload = new HashMap<>();

  private Map<String, Map<Integer, Part>> partList = new HashMap<>();

  private ArrayList<OzoneAcl> aclList = new ArrayList<>();
  private ReplicationConfig replicationConfig;

  public static Builder newBuilder() {
    return new Builder();
  }

  private OzoneBucketStub(Builder b) {
    super(b);
    this.replicationConfig = super.getReplicationConfig();
  }

  /**
   * Inner builder for OzoneBucketStub.
   */
  public static final class Builder extends OzoneBucket.Builder {

    private Builder() {
    }

    @Override
    public OzoneBucketStub build() {
      return new OzoneBucketStub(this);
    }
  }
  
  @Override
  public OzoneOutputStream createKey(String key, long size) throws IOException {
    return createKey(key, size,
        ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
        ReplicationFactor.ONE), new HashMap<>());
  }

  @Override
  public OzoneOutputStream createKey(String key, long size,
                                     ReplicationType type,
                                     ReplicationFactor factor,
                                     Map<String, String> metadata)
      throws IOException {
    ReplicationConfig replication = ReplicationConfig.fromTypeAndFactor(type, factor);
    return createKey(key, size, replication, metadata);
  }

  @Override
  public OzoneOutputStream createKey(String key, long size,
      ReplicationConfig rConfig, Map<String, String> metadata,
      Map<String, String> tags)
      throws IOException {
    assertDoesNotExist(key + "/");

    final ReplicationConfig repConfig;
    if (rConfig == null) {
      repConfig = getReplicationConfig();
    } else {
      repConfig = rConfig;
    }
    ReplicationConfig finalReplicationCon = repConfig;
    KeyMetadataAwareOutputStream keyOutputStream =
        new KeyMetadataAwareOutputStream(metadata) {
          @Override
          public void close() throws IOException {
            keyContents.put(key, toByteArray());
            keyDetails.put(key, new OzoneKeyDetails(
                getVolumeName(),
                getName(),
                key,
                size,
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                new ArrayList<>(), finalReplicationCon, getMetadata(), null,
                () -> readKey(key), true,
                UserGroupInformation.getCurrentUser().getShortUserName(),
                tags
            ));
            super.close();
          }
        };

    return new OzoneOutputStream(keyOutputStream, null);
  }

  @Override
  public OzoneOutputStream rewriteKey(String keyName, long size, long existingKeyGeneration,
      ReplicationConfig rConfig, Map<String, String> metadata) throws IOException {
    final ReplicationConfig repConfig;
    if (rConfig == null) {
      repConfig = getReplicationConfig();
    } else {
      repConfig = rConfig;
    }
    ReplicationConfig finalReplicationCon = repConfig;
    KeyMetadataAwareOutputStream byteArrayOutputStream =
        new KeyMetadataAwareOutputStream(metadata) {
          @Override
          public void close() throws IOException {
            keyContents.put(keyName, toByteArray());
            keyDetails.put(keyName, new OzoneKeyDetails(
                getVolumeName(),
                getName(),
                keyName,
                size,
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                new ArrayList<>(), finalReplicationCon, metadata, null,
                () -> readKey(keyName), true, null, null
            ));
            super.close();
          }
        };

    return new OzoneOutputStream(byteArrayOutputStream, null);
  }

  @Override
  public OzoneDataStreamOutput createStreamKey(String key, long size,
                                               ReplicationConfig rConfig,
                                               Map<String, String> keyMetadata,
                                               Map<String, String> tags)
      throws IOException {
    assertDoesNotExist(key + "/");

    ByteBufferStreamOutput byteBufferStreamOutput =
        new KeyMetadataAwareByteBufferStreamOutput(keyMetadata) {

          private final ByteBuffer buffer = ByteBuffer.allocate((int) size);

          @Override
          public void close() throws IOException {
            super.close();

            buffer.flip();
            byte[] bytes1 = new byte[buffer.remaining()];
            buffer.get(bytes1);
            keyContents.put(key, bytes1);

            Map<String, String> objectMetadata = keyMetadata == null ?
                new HashMap<>() : keyMetadata;

            keyDetails.put(key, new OzoneKeyDetails(
                getVolumeName(),
                getName(),
                key,
                size,
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                new ArrayList<>(), rConfig, objectMetadata, null,
                null, false,
                UserGroupInformation.getCurrentUser().getShortUserName(),
                tags
            ));
          }

          @Override
          public void write(ByteBuffer b, int off, int len)
              throws IOException {
            byte[] bytes = new byte[len];
            b.get(bytes, off, len);
            buffer.put(bytes);
          }

          @Override
          public void flush() throws IOException {
          }
        };

    return new OzoneDataStreamOutputStub(byteBufferStreamOutput, key + size);
  }

  @Override
  public OzoneDataStreamOutput createMultipartStreamKey(String key,
                                                        long size,
                                                        int partNumber,
                                                        String uploadID)
      throws IOException {
    MultipartInfoStub multipartInfo = keyToMultipartUpload.get(key);
    if (multipartInfo == null || !multipartInfo.getUploadId().equals(uploadID)) {
      throw new OMException(ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
    } else {
      ByteBufferStreamOutput byteBufferStreamOutput =
          new KeyMetadataAwareByteBufferStreamOutput(new HashMap<>()) {
            private final ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);

            @Override
            public void close() throws IOException {
              super.close();

              int position = buffer.position();
              buffer.flip();
              byte[] bytes = new byte[position];
              buffer.get(bytes);

              Part part = new Part(key + size, bytes,
                  getMetadata().get(ETAG));
              if (partList.get(key) == null) {
                Map<Integer, Part> parts = new TreeMap<>();
                parts.put(partNumber, part);
                partList.put(key, parts);
              } else {
                partList.get(key).put(partNumber, part);
              }
            }

            @Override
            public void write(ByteBuffer b, int off, int len)
                throws IOException {
              byte[] bytes = new byte[len];
              b.get(bytes, off, len);
              buffer.put(bytes);
            }

          };

      return new OzoneDataStreamOutputStub(byteBufferStreamOutput, key + size);
    }
  }

  @Override
  public OzoneInputStream readKey(String key) throws IOException {
    return new OzoneInputStream(new ByteArrayInputStream(keyContents.get(key)));
  }

  @Override
  public OzoneKeyDetails getKey(String key) throws IOException {
    if (keyDetails.containsKey(key)) {
      return keyDetails.get(key);
    } else {
      throw new OMException(ResultCodes.KEY_NOT_FOUND);
    }
  }

  @Override
  public OzoneKey headObject(String key) throws IOException {
    if (keyDetails.containsKey(key)) {
      OzoneKeyDetails ozoneKeyDetails = keyDetails.get(key);
      return new OzoneKey(ozoneKeyDetails.getVolumeName(),
          ozoneKeyDetails.getBucketName(),
          ozoneKeyDetails.getName(),
          ozoneKeyDetails.getDataSize(),
          ozoneKeyDetails.getCreationTime().toEpochMilli(),
          ozoneKeyDetails.getModificationTime().toEpochMilli(),
          ozoneKeyDetails.getReplicationConfig(),
          ozoneKeyDetails.getMetadata(),
          ozoneKeyDetails.isFile(),
          ozoneKeyDetails.getOwner(),
          ozoneKeyDetails.getTags());
    } else {
      throw new OMException(ResultCodes.KEY_NOT_FOUND);
    }
  }

  @Override
  public Iterator<? extends OzoneKey> listKeys(String keyPrefix) {
    Map<String, OzoneKey> sortedKey = new TreeMap<String, OzoneKey>(keyDetails);
    return sortedKey.values()
        .stream()
        .filter(key -> key.getName().startsWith(keyPrefix))
        .collect(Collectors.toList())
        .iterator();
  }

  @Override
  public Iterator<? extends OzoneKey> listKeys(String keyPrefix,
      String prevKey) {
    Map<String, OzoneKey> sortedKey = new TreeMap<String, OzoneKey>(keyDetails);
    return sortedKey.values()
        .stream()
        .filter(key -> key.getName().compareTo(prevKey) > 0)
        .filter(key -> key.getName().startsWith(keyPrefix))
        .collect(Collectors.toList())
        .iterator();
  }

  @Override
  public Iterator<? extends OzoneKey> listKeys(String keyPrefix,
      String prevKey, boolean shallow) throws IOException {
    if (!shallow) {
      return prevKey == null ? listKeys(keyPrefix)
          : listKeys(keyPrefix, prevKey);
    }

    Map<String, OzoneKey> sortedKey = new TreeMap<>(keyDetails);
    List<OzoneKey> ozoneKeys = sortedKey.values()
        .stream()
        .filter(key -> key.getName().startsWith(keyPrefix))
        .map(key -> {
          String[] res = key.getName().split(OZONE_URI_DELIMITER);
          String newKeyName;
          if (res.length < 2) {
            newKeyName = key.getName();
          } else if (res.length == 2) {
            newKeyName = res[0] + OZONE_URI_DELIMITER + res[1];
          } else {
            newKeyName =
                res[0] + OZONE_URI_DELIMITER + res[1] + OZONE_URI_DELIMITER;
          }
          return new OzoneKey(key.getVolumeName(),
              key.getBucketName(), newKeyName,
              key.getDataSize(),
              key.getCreationTime().getEpochSecond() * 1000,
              key.getModificationTime().getEpochSecond() * 1000,
              key.getReplicationConfig(), key.isFile(), key.getOwner());
        }).collect(Collectors.toList());

    if (prevKey != null) {
      return ozoneKeys.stream()
          .filter(key -> key.getName().compareTo(prevKey) > 0)
          .collect(Collectors.toList())
          .iterator();
    }
    return ozoneKeys.iterator();
  }

  @Override
  public void deleteKey(String key) throws IOException {
    keyDetails.remove(key);
  }

  @Override
  public Map<String, ErrorInfo> deleteKeys(List<String> keyList, boolean quiet) throws IOException {
    Map<String, ErrorInfo> keyErrorMap = new HashMap<>();
    for (String key : keyList) {
      if (keyDetails.remove(key) == null) {
        keyErrorMap.put(key, new ErrorInfo("KEY_NOT_FOUND", "Key does not exist"));
      }
    }
    return keyErrorMap;
  }

  @Override
  public void renameKey(String fromKeyName, String toKeyName)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(String keyName,
                                                 ReplicationType type,
                                                 ReplicationFactor factor)
      throws IOException {
    return initiateMultipartUpload(keyName, ReplicationConfig.fromTypeAndFactor(type, factor),
        Collections.emptyMap());
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(String keyName,
      ReplicationConfig repConfig) throws IOException {
    return initiateMultipartUpload(keyName, repConfig, Collections.emptyMap());
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(String keyName,
       ReplicationConfig config, Map<String, String> metadata, Map<String, String> tags)
      throws IOException {
    String uploadID = UUID.randomUUID().toString();
    keyToMultipartUpload.put(keyName, new MultipartInfoStub(uploadID, metadata, tags));
    return new OmMultipartInfo(getVolumeName(), getName(), keyName, uploadID);
  }

  @Override
  public OzoneOutputStream createMultipartKey(String key, long size,
                                              int partNumber, String uploadID)
      throws IOException {
    MultipartInfoStub multipartInfo = keyToMultipartUpload.get(key);
    if (multipartInfo == null || !multipartInfo.getUploadId().equals(uploadID)) {
      throw new OMException(ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
    } else {
      KeyMetadataAwareOutputStream keyOutputStream =
          new KeyMetadataAwareOutputStream((int) size, new HashMap<>()) {
            @Override
            public void close() throws IOException {
              Part part = new Part(key + size,
                  toByteArray(), getMetadata().get(ETAG));
              if (partList.get(key) == null) {
                Map<Integer, Part> parts = new TreeMap<>();
                parts.put(partNumber, part);
                partList.put(key, parts);
              } else {
                partList.get(key).put(partNumber, part);
              }
              super.close();
            }
          };
      return new OzoneOutputStreamStub(keyOutputStream, key + size);
    }
  }

  @Override
  public OmMultipartUploadCompleteInfo completeMultipartUpload(String key,
      String uploadID, Map<Integer, String> partsMap) throws IOException {

    if (keyToMultipartUpload.get(key) == null) {
      throw new OMException(ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
    } else {
      final Map<Integer, Part> partsList = partList.get(key);

      ByteArrayOutputStream output = new ByteArrayOutputStream();

      int prevPartNumber = 0;
      for (Map.Entry<Integer, String> part: partsMap.entrySet()) {
        int currentPartNumber = part.getKey();
        if (currentPartNumber <= prevPartNumber) {
          throw new OMException(OMException.ResultCodes.INVALID_PART_ORDER);
        }
        prevPartNumber = currentPartNumber;
      }
      for (Map.Entry<Integer, String> part: partsMap.entrySet()) {
        Part recordedPart = partsList.get(part.getKey());
        if (recordedPart == null ||
            !recordedPart.getETag().equals(part.getValue())) {
          throw new OMException(ResultCodes.INVALID_PART);
        } else {
          output.write(recordedPart.getContent());
        }
        keyContents.put(key, output.toByteArray());
      }

      keyDetails.put(key, new OzoneKeyDetails(
          getVolumeName(),
          getName(),
          key,
          keyContents.get(key) != null ? keyContents.get(key).length : 0,
          System.currentTimeMillis(),
          System.currentTimeMillis(),
          new ArrayList<>(), getReplicationConfig(),
          keyToMultipartUpload.get(key).getMetadata(), null,
          () -> readKey(key), true,
          UserGroupInformation.getCurrentUser().getShortUserName(),
          keyToMultipartUpload.get(key).getTags()
      ));
    }

    return new OmMultipartUploadCompleteInfo(getVolumeName(), getName(), key,
        DigestUtils.sha256Hex(key));
  }

  @Override
  public void abortMultipartUpload(String keyName, String uploadID) throws
      IOException {
    if (keyToMultipartUpload.get(keyName) == null) {
      throw new OMException(ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
    } else {
      keyToMultipartUpload.remove(keyName);
    }
  }

  @Override
  public OzoneMultipartUploadPartListParts listParts(String key,
      String uploadID, int partNumberMarker, int maxParts) throws IOException {
    if (keyToMultipartUpload.get(key) == null) {
      throw new OMException(ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
    }
    List<PartInfo> partInfoList = new ArrayList<>();

    if (partList.get(key) == null) {
      return new OzoneMultipartUploadPartListParts(
          RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
          0, false);
    } else {
      Map<Integer, Part> partMap = partList.get(key);
      Iterator<Map.Entry<Integer, Part>> partIterator =
          partMap.entrySet().iterator();

      int count = 0;
      int nextPartNumberMarker = 0;
      boolean truncated = false;
      MessageDigest eTagProvider;
      try {
        eTagProvider = MessageDigest.getInstance(MD5_HASH);
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
      while (count < maxParts && partIterator.hasNext()) {
        Map.Entry<Integer, Part> partEntry = partIterator.next();
        nextPartNumberMarker = partEntry.getKey();
        if (partEntry.getKey() > partNumberMarker) {
          PartInfo partInfo = new PartInfo(partEntry.getKey(),
              partEntry.getValue().getPartName(),
              Time.now(), partEntry.getValue().getContent().length,
              DatatypeConverter.printHexBinary(eTagProvider.digest(partEntry
                  .getValue().getContent())).toLowerCase());
          partInfoList.add(partInfo);
          count++;
        }
      }

      if (partIterator.hasNext()) {
        truncated = true;
      } else {
        truncated = false;
        nextPartNumberMarker = 0;
      }

      OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
          new OzoneMultipartUploadPartListParts(replicationConfig,
              nextPartNumberMarker, truncated);
      ozoneMultipartUploadPartListParts.addAllParts(partInfoList);

      return ozoneMultipartUploadPartListParts;

    }
  }

  @Override
  public List<OzoneAcl> getAcls() throws IOException {
    return (List<OzoneAcl>)aclList.clone();
  }

  @Override
  public boolean removeAcl(OzoneAcl removeAcl) throws IOException {
    return aclList.remove(removeAcl);
  }

  @Override
  public boolean addAcl(OzoneAcl addAcl) throws IOException {
    return aclList.add(addAcl);
  }

  @Override
  public boolean setAcl(List<OzoneAcl> acls) throws IOException {
    aclList.clear();
    return aclList.addAll(acls);
  }

  @Override
  public Map<String, String> getObjectTagging(String keyName) throws IOException {
    if (keyDetails.containsKey(keyName)) {
      OzoneKeyDetails ozoneKeyDetails = keyDetails.get(keyName);
      return ozoneKeyDetails.getTags();
    } else {
      throw new OMException(ResultCodes.KEY_NOT_FOUND);
    }
  }

  @Override
  public void putObjectTagging(String keyName, Map<String, String> tags) throws IOException {
    if (keyDetails.containsKey(keyName)) {
      OzoneKeyDetails ozoneKeyDetails = keyDetails.get(keyName);
      ozoneKeyDetails.getTags().clear();
      ozoneKeyDetails.getTags().putAll(tags);
    } else {
      throw new OMException(ResultCodes.KEY_NOT_FOUND);
    }
  }

  @Override
  public void deleteObjectTagging(String keyName) throws IOException {
    if (keyDetails.containsKey(keyName)) {
      OzoneKeyDetails ozoneKeyDetails = keyDetails.get(keyName);
      ozoneKeyDetails.getTags().clear();
    } else {
      throw new OMException(ResultCodes.KEY_NOT_FOUND);
    }
  }

  /**
   * Class used to hold part information in a upload part request.
   */
  public static class Part {
    private String partName;
    private byte[] content;

    private String eTag;

    public Part(String name, byte[] data, String eTag) {
      this.partName = name;
      this.content = data.clone();
      this.eTag = eTag;
    }

    public String getPartName() {
      return partName;
    }

    public byte[] getContent() {
      return content.clone();
    }

    public String getETag() {
      return eTag;
    }

  }

  @Override
  public void setReplicationConfig(ReplicationConfig replicationConfig) {
    this.replicationConfig = replicationConfig;
  }

  @Override
  public ReplicationConfig getReplicationConfig() {
    return this.replicationConfig;
  }

  @Override
  public void createDirectory(String keyName) throws IOException {
    assertDoesNotExist(StringUtils.stripEnd(keyName, "/"));

    LOG.info("createDirectory({})", keyName);
    keyDetails.put(keyName, new OzoneKeyDetails(
        getVolumeName(),
        getName(),
        keyName,
        0,
        System.currentTimeMillis(),
        System.currentTimeMillis(),
        new ArrayList<>(), replicationConfig, new HashMap<>(), null,
        () -> readKey(keyName), false,
        UserGroupInformation.getCurrentUser().getShortUserName(),
        Collections.emptyMap()));
  }

  private void assertDoesNotExist(String keyName) throws OMException {
    if (keyDetails.get(keyName) != null) {
      throw new OMException("already exists", ResultCodes.FILE_ALREADY_EXISTS);
    }
  }

  /**
   * ByteArrayOutputStream stub with metadata and support for pre-commit hooks.
   * This extends KeyOutputStream to allow OzoneOutputStream.getKeyOutputStream() to return a non-null value
   * and supports pre-commit hooks like Content-MD5 validation.
   */
  public static class KeyMetadataAwareOutputStream extends KeyOutputStream implements KeyMetadataAware {
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private final Map<String, String> metadata;
    private List<CheckedRunnable<IOException>> preCommits = Collections.emptyList();

    public KeyMetadataAwareOutputStream(Map<String, String> metadata) {
      super(null, null);
      this.metadata = metadata;
    }

    public KeyMetadataAwareOutputStream(int size,
                                        Map<String, String> metadata) {
      super(null, null);
      this.metadata = metadata;
    }

    @Override
    public void write(int b) throws IOException {
      buffer.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      buffer.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      buffer.flush();
    }

    @Override
    public void close() throws IOException {
      // Run pre-commit hooks before closing (e.g., Content-MD5 validation)
      for (CheckedRunnable<IOException> preCommit : preCommits) {
        preCommit.run();
      }
      buffer.close();
    }

    @Override
    public void setPreCommits(List<CheckedRunnable<IOException>> preCommits) {
      this.preCommits = preCommits != null ? preCommits : Collections.emptyList();
    }

    public byte[] toByteArray() {
      return buffer.toByteArray();
    }

    @Override
    public Map<String, String> getMetadata() {
      return metadata;
    }
  }

  /**
   * ByteBufferOutputStream stub with metadata and support for pre-commit hooks.
   * This extends KeyDataStreamOutput to allow OzoneDataStreamOutput.getKeyDataStreamOutput() to return a non-null value
   * and supports pre-commit hooks like Content-MD5 validation.
   */
  public static class KeyMetadataAwareByteBufferStreamOutput
      extends KeyDataStreamOutput implements KeyMetadataAware {

    private final Map<String, String> metadata;
    private List<CheckedRunnable<IOException>> preCommits = Collections.emptyList();

    public KeyMetadataAwareByteBufferStreamOutput(
        Map<String, String> metadata) {
      super();
      this.metadata = metadata;
    }

    @Override
    public void write(ByteBuffer buffer, int off, int len) throws IOException {

    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public void close() throws IOException {

      for (CheckedRunnable<IOException> preCommit : preCommits) {
        preCommit.run();
      }
    }

    @Override
    public void hflush() {

    }

    @Override
    public void hsync() throws IOException {

    }

    @Override
    public void setPreCommits(List<CheckedRunnable<IOException>> preCommits) {
      this.preCommits = preCommits != null ? preCommits : Collections.emptyList();
    }

    @Override
    public Map<String, String> getMetadata() {
      return metadata;
    }
  }

  /**
   * Multipart upload stub to store MPU related information.
   */
  private static class MultipartInfoStub {

    private final String uploadId;
    private final Map<String, String> metadata;
    private final Map<String, String> tags;

    MultipartInfoStub(String uploadId, Map<String, String> metadata,
                      Map<String, String> tags) {
      this.uploadId = uploadId;
      this.metadata = metadata;
      this.tags = tags;
    }

    public String getUploadId() {
      return uploadId;
    }

    public Map<String, String> getMetadata() {
      return metadata;
    }

    public Map<String, String> getTags() {
      return tags;
    }
  }

}
