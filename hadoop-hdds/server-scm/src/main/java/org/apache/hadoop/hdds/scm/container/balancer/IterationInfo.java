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

package org.apache.hadoop.hdds.scm.container.balancer;

/**
 * Information about the iteration.
 */
public class IterationInfo {

  private final Integer iterationNumber;
  private final String iterationResult;
  private final Long iterationDuration;

  public IterationInfo(Integer iterationNumber, String iterationResult, long iterationDuration) {
    this.iterationNumber = iterationNumber;
    this.iterationResult = iterationResult;
    this.iterationDuration = iterationDuration;
  }

  public Integer getIterationNumber() {
    return iterationNumber;
  }

  public String getIterationResult() {
    return iterationResult;
  }

  public Long getIterationDuration() {
    return iterationDuration;
  }
}
