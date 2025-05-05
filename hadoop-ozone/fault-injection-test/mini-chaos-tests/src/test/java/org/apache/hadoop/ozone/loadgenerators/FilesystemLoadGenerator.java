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
 * Filesystem load generator for Ozone.
 *
 * This load generator read, writes and deletes data using the filesystem
 * apis.
 */
public class FilesystemLoadGenerator extends LoadGenerator {

  private final LoadBucket fsBucket;
  private final DataBuffer dataBuffer;

  public FilesystemLoadGenerator(DataBuffer dataBuffer, LoadBucket fsBucket) {
    this.dataBuffer = dataBuffer;
    this.fsBucket = fsBucket;
  }

  @Override
  public void generateLoad() throws Exception {
    int index = RandomUtils.secure().randomInt();
    ByteBuffer buffer = dataBuffer.getBuffer(index);
    String keyName = getKeyName(index);
    fsBucket.writeKey(true, buffer, keyName);

    fsBucket.readKey(true, buffer, keyName);

    fsBucket.deleteKey(true, keyName);
  }

  @Override
  public void initialize() {
    // Nothing to do here
  }
}
