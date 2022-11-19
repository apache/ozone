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
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.loadgenerators.DataBuffer;
import org.apache.hadoop.ozone.loadgenerators.LoadExecutors;
import org.apache.hadoop.ozone.loadgenerators.LoadGenerator;
import org.apache.hadoop.ozone.loadgenerators.LoadBucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A Simple Load generator for testing.
 */
public final class MiniOzoneLoadGenerator {

  private static final Logger LOG =
      LoggerFactory.getLogger(MiniOzoneLoadGenerator.class);

  private final List<LoadGenerator> loadGenerators;
  private final LoadExecutors loadExecutor;

  private final OzoneVolume volume;
  private final OzoneConfiguration conf;
  private final String omServiceID;
  private final BucketArgs bucketArgs;

  private MiniOzoneLoadGenerator(OzoneVolume volume, int numThreads,
      int numBuffers, OzoneConfiguration conf, String omServiceId,
      BucketArgs bucketArgs, Set<Class<? extends LoadGenerator>>
      loadGeneratorClazzes) throws Exception {
    DataBuffer buffer = new DataBuffer(numBuffers);
    loadGenerators = new ArrayList<>();
    this.volume = volume;
    this.conf = conf;
    this.omServiceID = omServiceId;
    this.bucketArgs = bucketArgs;

    for (Class<? extends LoadGenerator> clazz : loadGeneratorClazzes) {
      addLoads(clazz, buffer);
    }

    this.loadExecutor = new LoadExecutors(numThreads, loadGenerators);
  }

  private void addLoads(Class<? extends LoadGenerator> clazz,
                        DataBuffer buffer) throws Exception {
    String bucketName = RandomStringUtils.randomAlphabetic(10).toLowerCase();

    volume.createBucket(bucketName, bucketArgs);
    LoadBucket ozoneBucket = new LoadBucket(volume.getBucket(bucketName),
        conf, omServiceID);

    LoadGenerator loadGenerator = clazz
        .getConstructor(DataBuffer.class, LoadBucket.class)
        .newInstance(buffer, ozoneBucket);
    loadGenerators.add(loadGenerator);
  }

  void startIO(long time, TimeUnit timeUnit) throws Exception {
    LOG.info("Starting MiniOzoneLoadGenerator for time {}:{}", time, timeUnit);
    long runTime = timeUnit.toMillis(time);
    // start and wait for executors to finish
    loadExecutor.startLoad(runTime);
    loadExecutor.waitForCompletion();
  }

  void shutdownLoadGenerator() {
    loadExecutor.shutdown();
  }

  /**
   * Builder to create Ozone load generator.
   */
  public static class Builder {
    private Set<Class<? extends LoadGenerator>> clazzes = new HashSet<>();
    private String omServiceId;
    private OzoneConfiguration conf;
    private int numBuffers;
    private int numThreads;
    private OzoneVolume volume;
    private BucketArgs bucketArgs;

    public Builder addLoadGenerator(Class<? extends LoadGenerator> clazz) {
      clazzes.add(clazz);
      return this;
    }

    public Builder setOMServiceId(String serviceId) {
      omServiceId = serviceId;
      return this;
    }

    public Builder setConf(OzoneConfiguration configuration) {
      this.conf = configuration;
      return this;
    }

    public Builder setNumBuffers(int buffers) {
      this.numBuffers = buffers;
      return this;
    }

    public Builder setNumThreads(int threads) {
      this.numThreads = threads;
      return this;
    }

    public Builder setVolume(OzoneVolume vol) {
      this.volume = vol;
      return this;
    }

    public Builder setBucketArgs(BucketArgs buckArgs) {
      this.bucketArgs = buckArgs;
      return this;
    }

    public MiniOzoneLoadGenerator build() throws Exception {
      return new MiniOzoneLoadGenerator(volume, numThreads, numBuffers,
          conf, omServiceId, bucketArgs, clazzes);
    }
  }
}
