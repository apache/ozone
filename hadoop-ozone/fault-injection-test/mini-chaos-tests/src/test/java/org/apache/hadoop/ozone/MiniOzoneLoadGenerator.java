/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.loadgenerators.FilesystemLoadGenerator;
import org.apache.hadoop.ozone.loadgenerators.AgedLoadGenerator;
import org.apache.hadoop.ozone.loadgenerators.RandomLoadGenerator;
import org.apache.hadoop.ozone.loadgenerators.DataBuffer;
import org.apache.hadoop.ozone.loadgenerators.LoadExecutors;
import org.apache.hadoop.ozone.loadgenerators.LoadGenerator;
import org.apache.hadoop.ozone.utils.LoadBucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.concurrent.TimeUnit;

/**
 * A Simple Load generator for testing.
 */
public class MiniOzoneLoadGenerator {

  private static final Logger LOG =
      LoggerFactory.getLogger(MiniOzoneLoadGenerator.class);

  private final List<LoadExecutors> loadExecutors;

  private final OzoneVolume volume;
  private final OzoneConfiguration conf;
  private final String omServiceID;

  MiniOzoneLoadGenerator(OzoneVolume volume, int numClients, int numThreads,
      int numBuffers, OzoneConfiguration conf, String omServiceId)
      throws Exception {
    DataBuffer buffer = new DataBuffer(numBuffers);
    loadExecutors = new ArrayList<>();
    this.volume = volume;
    this.conf = conf;
    this.omServiceID = omServiceId;

    // Random Load
    String mixBucketName = RandomStringUtils.randomAlphabetic(10).toLowerCase();
    volume.createBucket(mixBucketName);
    List<LoadBucket> ozoneBuckets = new ArrayList<>(numClients);
    for (int i = 0; i < numClients; i++) {
      ozoneBuckets.add(new LoadBucket(volume.getBucket(mixBucketName),
          conf, omServiceId));
    }
    RandomLoadGenerator loadGenerator =
        new RandomLoadGenerator(buffer, ozoneBuckets);
    loadExecutors.add(new LoadExecutors(numThreads, loadGenerator));

    // Aged Load
    addLoads(numThreads,
        bucket -> new AgedLoadGenerator(buffer, bucket));

    //Filesystem Load
    addLoads(numThreads,
        bucket -> new FilesystemLoadGenerator(buffer, bucket));
  }

  private void addLoads(int numThreads,
                        Function<LoadBucket, LoadGenerator> function)
      throws Exception {
    String bucketName = RandomStringUtils.randomAlphabetic(10).toLowerCase();
    volume.createBucket(bucketName);
    LoadBucket bucket = new LoadBucket(volume.getBucket(bucketName), conf,
        omServiceID);
    LoadGenerator loadGenerator = function.apply(bucket);
    loadExecutors.add(new LoadExecutors(numThreads, loadGenerator));
  }

  void startIO(long time, TimeUnit timeUnit) {
    LOG.info("Starting MiniOzoneLoadGenerator for time {}:{}", time, timeUnit);
    long runTime = timeUnit.toMillis(time);
    // start and wait for executors to finish
    loadExecutors.forEach(le -> le.startLoad(runTime));
    loadExecutors.forEach(LoadExecutors::waitForCompletion);
  }

  void shutdownLoadGenerator() {
    loadExecutors.forEach(LoadExecutors::shutdown);
  }
}
