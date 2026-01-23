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

import java.util.ArrayList;
import java.util.List;

/**
 * Interface for load generator.
 */
public abstract class LoadGenerator {

  private static final String KEY_NAME_DELIMITER = "_";

  public static List<Class<? extends LoadGenerator>> getClassList() {
    List<Class<? extends LoadGenerator>> classList = new ArrayList<>();

    classList.add(AgedDirLoadGenerator.class);
    classList.add(AgedLoadGenerator.class);
    classList.add(FilesystemLoadGenerator.class);
    classList.add(NestedDirLoadGenerator.class);
    classList.add(RandomDirLoadGenerator.class);
    classList.add(RandomLoadGenerator.class);
    classList.add(ReadOnlyLoadGenerator.class);

    return classList;
  }

  /*
   * The implemented LoadGenerators constructors should have the
   * constructor with the signature as following
   * class NewLoadGen implements LoadGenerator {
   *
   *   NewLoadGen(DataBuffer buffer, LoadBucket bucket) {
   *     // Add code here
   *   }
   * }
   */

  public abstract void initialize() throws Exception;

  public abstract void generateLoad() throws Exception;

  String getKeyName(int keyIndex) {
    return toString() + KEY_NAME_DELIMITER + keyIndex;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
