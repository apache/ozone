/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.shell.volume;

import org.apache.hadoop.ozone.OzoneConsts;
import picocli.CommandLine;

/**
 * Quota parameter for volume command.
 **/
public class VolumeQuota {
  @CommandLine.Option(names = {"--nsQuota", "-nsq"},
      description = "Set Namespace Quota of " +
          "specific volume (eg. 5)")
  private long nsQuota = OzoneConsts.QUOTA_COUNT_RESET;

  @CommandLine.Option(names = {"--ssQuota", "-ssq"},
      description = "Quota of the newly " +
          "created volume (eg. 5G)")
  private String ssQuota;

  public long getNsQuota() {
    return nsQuota;
  }

  public String getSsQuota() {
    return ssQuota;
  }
}
