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

package org.apache.hadoop.ozone.s3.endpoint;

/**
 * Result of a copy operation.
 */
public class CopyResult {
  private final String eTag;
  private final long size;
  private final long modificationTime;

  public CopyResult(String eTag, long size, long modificationTime) {
    this.eTag = eTag;
    this.size = size;
    this.modificationTime = modificationTime;
  }

  public String getETag() {
    return eTag;
  }

  public long getSize() {
    return size;
  }

  public long getModificationTime() {
    return modificationTime;
  }
}
