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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.util.Time;

/**
 * ObjectStore implementation with in-memory state.
 */
public class ObjectStoreStub extends ObjectStore {

  private final Map<String, OzoneVolumeStub> volumes = new HashMap<>();
  private final String s3VolumeName;

  public ObjectStoreStub() {
    this(new OzoneConfiguration(), mock(ClientProtocol.class));
  }

  public ObjectStoreStub(ConfigurationSource conf, ClientProtocol proxy) {
    super(conf, proxy);
    s3VolumeName = HddsClientUtils.getDefaultS3VolumeName(conf);
  }

  @Override
  public void createVolume(String volumeName) {
    createVolume(volumeName,
        VolumeArgs.newBuilder()
            .setAdmin("root")
            .setOwner("root")
            .setQuotaInBytes(Integer.MAX_VALUE)
            .build());
  }

  @Override
  public void createVolume(String volumeName, VolumeArgs volumeArgs) {
    OzoneVolumeStub volume = OzoneVolumeStub.newBuilder()
        .setName(volumeName)
        .setAdmin(volumeArgs.getAdmin())
        .setOwner(volumeArgs.getOwner())
        .setQuotaInBytes(volumeArgs.getQuotaInBytes())
        .setQuotaInNamespace(volumeArgs.getQuotaInNamespace())
        .setCreationTime(Time.now())
        .setAcls(volumeArgs.getAcls())
        .build();
    volumes.put(volumeName, volume);
  }

  @Override
  public OzoneVolume getVolume(String volumeName) throws IOException {
    if (volumes.containsKey(volumeName)) {
      return volumes.get(volumeName);
    } else {
      throw new OMException("", VOLUME_NOT_FOUND);
    }
  }

  @Override
  public Iterator<? extends OzoneVolume> listVolumes(String volumePrefix) {
    return volumes.values()
        .stream()
        .filter(volume -> volume.getName().startsWith(volumePrefix))
        .collect(Collectors.toList())
        .iterator();

  }

  @Override
  public Iterator<? extends OzoneVolume> listVolumes(String volumePrefix,
      String prevVolume) {
    return volumes.values()
        .stream()
        .filter(volume -> volume.getName().compareTo(prevVolume) > 0)
        .filter(volume -> volume.getName().startsWith(volumePrefix))
        .collect(Collectors.toList())
        .iterator();
  }

  @Override
  public Iterator<? extends OzoneVolume> listVolumesByUser(String user,
      String volumePrefix, String prevVolume) {
    return volumes.values()
        .stream()
        .filter(volume -> volume.getOwner().equals(user))
        .filter(volume -> volume.getName().compareTo(prevVolume) < 0)
        .filter(volume -> volume.getName().startsWith(volumePrefix))
        .collect(Collectors.toList())
        .iterator();
  }

  @Override
  public void deleteVolume(String volumeName) {
    volumes.remove(volumeName);
  }

  @Override
  public OzoneVolume getS3Volume() throws IOException {
    // Always return default S3 volume. This class will not be used for
    // multitenant testing.
    return getVolume(s3VolumeName);
  }

  @Override
  public void createS3Bucket(String s3BucketName) throws
      IOException {
    if (!volumes.containsKey(s3VolumeName)) {
      createVolume(s3VolumeName);
    }
    volumes.get(s3VolumeName).createBucket(s3BucketName);
  }

  @Override
  public void deleteS3Bucket(String s3BucketName) throws
      IOException {
    OzoneVolume volume = getS3Volume();
    if (volume == null) {
      throw new OMException("", VOLUME_NOT_FOUND);
    }
    volume.deleteBucket(s3BucketName);
  }
}
