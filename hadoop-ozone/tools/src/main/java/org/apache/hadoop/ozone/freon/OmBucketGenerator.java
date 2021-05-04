/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.freon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;

import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Data generator tool test om performance.
 */
@Command(name = "ombg",
    aliases = "om-bucket-generator",
    description = "Generate ozone buckets on OM side.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class OmBucketGenerator extends BaseFreonGenerator
    implements Callable<Void> {
  private static final Logger LOG =
      LoggerFactory.getLogger(OmBucketGenerator.class);

  @Option(names = {"-v", "--volume"},
      description = "Name of the bucket which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "vol1")
  private String volumeName;

  @Option(
      names = "--om-service-id",
      description = "OM Service ID"
  )
  private String omServiceID = null;

  private OzoneManagerProtocol ozoneManagerClient;

  private Timer bucketCreationTimer;

  private List<String> bucketList;

  public OmBucketGenerator() {
    this.bucketList = Collections.synchronizedList(new ArrayList<String>());
  }

  @Override
  public Void call() throws Exception {

    init();

    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();

    try (OzoneClient rpcClient = createOzoneClient(omServiceID,
        ozoneConfiguration)) {
      ensureVolumeExists(rpcClient, volumeName);

      ozoneManagerClient = createOmClient(ozoneConfiguration, omServiceID);

      bucketCreationTimer = getMetrics().timer("bucket-create");

      runTests(this::createBucket);

    } finally {
      if (ozoneManagerClient != null) {
        ozoneManagerClient.close();
      }
    }

    return null;
  }

  private void createBucket(long index) throws Exception {

    OmBucketInfo bucketInfo = new OmBucketInfo.Builder()
        .setBucketName(getPrefix()+index)
        .setVolumeName(volumeName)
        .setStorageType(StorageType.DISK)
        .build();

    bucketList.add(bucketInfo.getBucketName());
    bucketCreationTimer.time(() -> {
      ozoneManagerClient.createBucket(bucketInfo);
      return null;
    });
  }

  @Override
  protected void doCleanUp() {
    LOG.info("Cleaning up generated objects.");
    try {
      for (String bucket : bucketList) {
        ozoneManagerClient.deleteBucket(volumeName, bucket);
      }
      if (isVolumeCreated()) {
        ozoneManagerClient.deleteVolume(volumeName);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
