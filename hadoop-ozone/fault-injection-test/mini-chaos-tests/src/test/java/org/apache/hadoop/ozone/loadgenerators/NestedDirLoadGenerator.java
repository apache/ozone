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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.RandomUtils;

/**
 * A Load generator where nested directories are created and read them.
 */
public class NestedDirLoadGenerator extends LoadGenerator {
  private final LoadBucket fsBucket;
  private final int maxDirDepth;
  private final Map<Integer, String> pathMap;

  public NestedDirLoadGenerator(DataBuffer dataBuffer, LoadBucket fsBucket) {
    this.fsBucket = fsBucket;
    this.maxDirDepth = 20;
    this.pathMap = new ConcurrentHashMap<>();
  }

  private String createNewPath(int i, String s) {
    String base = s != null ? s : "";
    return base + "/" + getKeyName(i);
  }

  @Override
  public void generateLoad() throws Exception {
    int index = RandomUtils.secure().randomInt(0, maxDirDepth);
    String str = this.pathMap.compute(index, this::createNewPath);
    fsBucket.createDirectory(str);
    fsBucket.readDirectory(str);
  }

  @Override
  public void initialize() throws Exception {
    // Nothing to do here
  }
}
