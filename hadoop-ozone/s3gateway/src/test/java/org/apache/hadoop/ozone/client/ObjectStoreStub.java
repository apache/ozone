/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.hadoop.ozone.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.util.Time;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_EMPTY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;

/**
 * ObjectStore implementation with in-memory state.
 */
public class ObjectStoreStub extends ObjectStore {

  public ObjectStoreStub() {
    super();
  }

  private Map<String, OzoneVolumeStub> volumes = new HashMap<>();
  private Map<String, Boolean> bucketEmptyStatus = new HashMap<>();
  private static OzoneConfiguration conf = new OzoneConfiguration();

  @Override
  public void createVolume(String volumeName) throws IOException {
    createVolume(volumeName,
        VolumeArgs.newBuilder()
            .setAdmin("root")
            .setOwner("root")
            .setQuota("" + Integer.MAX_VALUE)
            .setAcls(new ArrayList<>()).build());
  }

  @Override
  public void createVolume(String volumeName, VolumeArgs volumeArgs) {
    OzoneVolumeStub volume =
        new OzoneVolumeStub(volumeName,
            volumeArgs.getAdmin(),
            volumeArgs.getOwner(),
            Long.parseLong(volumeArgs.getQuota()),
            Time.now(),
            volumeArgs.getAcls());
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
  public Iterator<? extends OzoneVolume> listVolumes(String volumePrefix)
      throws IOException {
    return volumes.values()
        .stream()
        .filter(volume -> volume.getName().startsWith(volumePrefix))
        .collect(Collectors.toList())
        .iterator();

  }

  @Override
  public Iterator<? extends OzoneVolume> listVolumes(String volumePrefix,
      String prevVolume) throws IOException {
    return volumes.values()
        .stream()
        .filter(volume -> volume.getName().compareTo(prevVolume) > 0)
        .filter(volume -> volume.getName().startsWith(volumePrefix))
        .collect(Collectors.toList())
        .iterator();
  }

  @Override
  public Iterator<? extends OzoneVolume> listVolumesByUser(String user,
      String volumePrefix, String prevVolume) throws IOException {
    return volumes.values()
        .stream()
        .filter(volume -> volume.getOwner().equals(user))
        .filter(volume -> volume.getName().compareTo(prevVolume) < 0)
        .filter(volume -> volume.getName().startsWith(volumePrefix))
        .collect(Collectors.toList())
        .iterator();
  }

  @Override
  public void deleteVolume(String volumeName) throws IOException {
    volumes.remove(volumeName);
  }

  @Override
  public void createS3Bucket(String s3BucketName) throws
      IOException {
    if (!bucketEmptyStatus.containsKey(s3BucketName)) {
      String volumeName = HddsClientUtils.getS3VolumeName(conf);
      bucketEmptyStatus.put(s3BucketName, true);
      if (!volumes.containsKey(volumeName)) {
        createVolume(volumeName);
      }
      volumes.get(volumeName).createBucket(s3BucketName);
    } else {
      throw new OMException("", BUCKET_ALREADY_EXISTS);
    }
  }

  @Override
  public void deleteS3Bucket(String s3BucketName) throws
      IOException {
    if (bucketEmptyStatus.containsKey(s3BucketName)) {
      if (bucketEmptyStatus.get(s3BucketName)) {
        bucketEmptyStatus.remove(s3BucketName);
      } else {
        throw new OMException("", BUCKET_NOT_EMPTY);
      }
    } else {
      throw new OMException("", BUCKET_NOT_FOUND);
    }
  }

  public void setBucketEmptyStatus(String bucketName, boolean status) {
    bucketEmptyStatus.computeIfPresent(bucketName, (k, v) -> status);
  }
}
