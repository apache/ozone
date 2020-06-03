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
import org.apache.hadoop.ozone.utils.LoadBucket;

/**
 * A simple directory based load generator.
 */
public class RandomDirLoadGenerator extends LoadGenerator {
  private final LoadBucket fsBucket;

  public RandomDirLoadGenerator(DataBuffer dataBuffer, LoadBucket fsBucket) {
    this.fsBucket = fsBucket;
  }

  @Override
  public void generateLoad() throws Exception {
    int index = RandomUtils.nextInt();
    String keyName = getKeyName(index);
    fsBucket.createDirectory(keyName);
    fsBucket.readDirectory(keyName);
  }

  @Override
  public void initialize() {
    // Nothing to do here
  }
}