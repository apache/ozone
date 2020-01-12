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

package org.apache.hadoop.ozone.loadgenerators;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.ozone.MiniOzoneLoadGenerator;
import org.apache.hadoop.ozone.utils.LoadBucket;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Filesystem load generator for Ozone.
 *
 * This load generator read, writes and deletes data using the filesystem
 * apis.
 */
public class FilesystemLoadGenerator implements LoadGenerator {
  private static final Logger LOG =
      LoggerFactory.getLogger(FilesystemLoadGenerator.class);


  private final LoadBucket fsBucket;
  private final DataBuffer dataBuffer;

  public FilesystemLoadGenerator(DataBuffer dataBuffer, LoadBucket fsBucket) {
    this.dataBuffer = dataBuffer;
    this.fsBucket = fsBucket;
  }

  // Start IO load on an Ozone bucket.
  public void startLoad(long runTimeMillis) {
    long threadID = Thread.currentThread().getId();
    LOG.info("Started Filesystem IO Thread:{}.", threadID);
    String threadName = Thread.currentThread().getName();
    long startTime = Time.monotonicNow();

    while (Time.monotonicNow() < startTime + runTimeMillis) {
      try {
        int index = RandomUtils.nextInt();
        ByteBuffer buffer = dataBuffer.getBuffer(index);
        String keyName = MiniOzoneLoadGenerator.getKeyName(index, threadName);
        fsBucket.writeKey(true, buffer, keyName);

        fsBucket.readKey(true, buffer, keyName);

        fsBucket.deleteKey(true, keyName);
      } catch (Throwable t) {
        LOG.error("LOADGEN: Exiting due to exception", t);
        ExitUtil.terminate(new ExitUtil.ExitException(1, t));
      }
    }
  }

  public void initialize() {
    // Nothing to do here
  }

  @Override
  public String name() {
    return "FileSystem Load";
  }
}
