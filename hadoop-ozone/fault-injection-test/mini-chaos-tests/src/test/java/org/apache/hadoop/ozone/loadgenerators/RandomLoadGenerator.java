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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Random load generator which writes, read and deletes keys from
 * the bucket.
 */
public class RandomLoadGenerator extends LoadGenerator {
  private static final Logger LOG =
      LoggerFactory.getLogger(RandomLoadGenerator.class);

  private final LoadBucket ozoneBucket;
  private final DataBuffer dataBuffer;

  public RandomLoadGenerator(DataBuffer dataBuffer, LoadBucket bucket) {
    this.ozoneBucket = bucket;
    this.dataBuffer = dataBuffer;
  }

  @Override
  public void generateLoad() throws Exception {
    int index = RandomUtils.nextInt();
    ByteBuffer buffer = dataBuffer.getBuffer(index);
    String keyName = getKeyName(index);
    ozoneBucket.writeKey(buffer, keyName);

    ozoneBucket.readKey(buffer, keyName);

    ozoneBucket.deleteKey(keyName);
  }

  @Override
  public void initialize() {
    // Nothing to do here
  }
}
