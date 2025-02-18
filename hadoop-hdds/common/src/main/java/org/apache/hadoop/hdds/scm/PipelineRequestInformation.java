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

package org.apache.hadoop.hdds.scm;

/**
 * The information of the request of pipeline.
 */
public final class PipelineRequestInformation {
  private final long size;

  /**
   * Builder for PipelineRequestInformation.
   */
  public static class Builder {
    private long size;

    public static Builder getBuilder() {
      return new Builder();
    }

    /**
     * sets the size.
     * @param sz request size
     * @return Builder for PipelineRequestInformation
     */
    public Builder setSize(long sz) {
      this.size = sz;
      return this;
    }

    public PipelineRequestInformation build() {
      return new PipelineRequestInformation(size);
    }
  }

  private PipelineRequestInformation(long size) {
    this.size = size;
  }

  public long getSize() {
    return size;
  }
}
