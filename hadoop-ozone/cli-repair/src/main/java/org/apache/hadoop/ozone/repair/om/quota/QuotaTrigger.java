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

package org.apache.hadoop.ozone.repair.om.quota;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.repair.RepairTool;
import picocli.CommandLine;

/**
 * Tool to trigger quota repair.
 */
@CommandLine.Command(
    name = "start",
    description = "CLI to trigger quota repair.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class QuotaTrigger extends RepairTool {

  @CommandLine.ParentCommand
  private QuotaRepair parent;

  @CommandLine.Option(
      names = {"--service-id", "--om-service-id"},
      description = "Ozone Manager Service ID",
      required = false
  )
  private String omServiceId;

  @CommandLine.Option(
      names = {"--service-host"},
      description = "Ozone Manager Host. If OM HA is enabled, use --service-id instead. "
          + "If you must use --service-host with OM HA, this must point directly to the leader OM. "
          + "This option is required when --service-id is not provided or when HA is not enabled."
  )
  private String omHost;

  @CommandLine.Option(names = {"--buckets"},
      required = false,
      description = "start quota repair for specific buckets. Input will be list of uri separated by comma as" +
          " /<volume>/<bucket>[,...]")
  private String buckets;

  @Override
  public void execute() throws Exception {
    List<String> bucketList = Collections.emptyList();
    if (StringUtils.isNotEmpty(buckets)) {
      bucketList = Arrays.asList(buckets.split(","));
    }

    try (OzoneManagerProtocol omClient = parent.createOmClient(omServiceId, omHost, false)) {
      info("Triggering quota repair for %s",
          bucketList.isEmpty()
              ? "all buckets"
              : ("buckets " + buckets));
      if (!isDryRun()) {
        omClient.startQuotaRepair(bucketList);
        info(omClient.getQuotaRepairStatus());
      }
    }
  }
}
