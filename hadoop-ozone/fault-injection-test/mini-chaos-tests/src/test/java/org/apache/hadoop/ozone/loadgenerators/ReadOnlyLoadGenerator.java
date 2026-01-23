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

package org.apache.hadoop.ozone.loadgenerators;

import java.nio.ByteBuffer;
import org.apache.commons.lang3.RandomUtils;

/**
 * This load generator writes some files and reads the same file multiple times.
 */
public class ReadOnlyLoadGenerator extends LoadGenerator {
  private final LoadBucket replBucket;
  private final DataBuffer dataBuffer;
  private static final int NUM_KEYS = 10;

  public ReadOnlyLoadGenerator(DataBuffer dataBuffer, LoadBucket replBucket) {
    this.dataBuffer = dataBuffer;
    this.replBucket = replBucket;
  }

  @Override
  public void generateLoad() throws Exception {
    int index = RandomUtils.secure().randomInt(0, NUM_KEYS);
    ByteBuffer buffer = dataBuffer.getBuffer(index);
    String keyName = getKeyName(index);
    replBucket.readKey(buffer, keyName);
  }

  @Override
  public void initialize() throws Exception {
    for (int index = 0; index < NUM_KEYS; index++) {
      ByteBuffer buffer = dataBuffer.getBuffer(index);
      String keyName = getKeyName(index);
      replBucket.writeKey(buffer, keyName);
    }
  }
}
