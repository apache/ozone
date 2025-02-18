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

package org.apache.hadoop.ozone.shell;

import picocli.CommandLine;

/**
 * Common options for 'quota' commands.
 */
public class SetSpaceQuotaOptions {

  // Added --quota for backward compatibility.
  @CommandLine.Option(names = {"--space-quota", "--quota"},
      description = "The maximum space quota can be used (eg. 1GB)")
  private String quotaInBytes;

  @CommandLine.Option(names = {"--namespace-quota"},
      description = "For volume this parameter represents the number of " +
          "buckets, and for buckets represents the number of keys (eg. 5)")
  private String quotaInNamespace;

  public String getQuotaInBytes() {
    return quotaInBytes;
  }

  public String getQuotaInNamespace() {
    return quotaInNamespace;
  }

}
