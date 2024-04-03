/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.admin.nssummary;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.server.JsonUtils;
import picocli.CommandLine;

import java.util.concurrent.Callable;

import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.makeHttpCall;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printEmptyPathRequest;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printBucketReminder;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printKVSeparator;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printNewLines;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printPathNotFound;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printSpaces;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printTypeNA;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printWithUnderline;

/**
 * Quota Usage Subcommand.
 */
@CommandLine.Command(
    name = "quota",
    description = "Get quota usage for a path request.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)

public class QuotaUsageSubCommand implements Callable {
  @CommandLine.ParentCommand
  private NSSummaryAdmin parent;

  @CommandLine.Parameters(index = "0", arity = "0..1",
      description = "Non-empty path request without any protocol prefix.")
  private String path;

  private static final String ENDPOINT = "/api/v1/namespace/quota";

  private StringBuffer url = new StringBuffer();

  @Override
  public Void call() throws Exception {
    if (path == null || path.length() == 0) {
      printEmptyPathRequest();
      return null;
    }
    url.append(parent.getReconWebAddress()).append(ENDPOINT);

    printNewLines(1);
    String response = makeHttpCall(url, path,
        parent.isHTTPSEnabled(), parent.getOzoneConfig());
    if (response == null) {
      printNewLines(1);
      return null;
    }

    JsonNode quotaResponse = JsonUtils.readTree(response);

    if ("PATH_NOT_FOUND".equals(quotaResponse.path("status").asText())) {
      printPathNotFound();
    } else if ("TYPE_NOT_APPLICABLE".equals(quotaResponse.path("status").asText())) {
      printTypeNA("Quota");
    } else {
      if (parent.isNotValidBucketOrOBSBucket(path)) {
        printBucketReminder();
      }

      printWithUnderline("Quota", true);

      long quotaAllowed = quotaResponse.get("allowed").asLong();
      long quotaUsed = quotaResponse.get("used").asLong();

      printSpaces(2);
      System.out.print("Allowed");
      printKVSeparator();
      if (quotaAllowed != -1) {
        System.out.println(FileUtils.byteCountToDisplaySize(quotaAllowed));
      } else {
        System.out.println("Quota not set");
      }

      printSpaces(2);
      System.out.print("Used");
      printKVSeparator();
      System.out.println(FileUtils.byteCountToDisplaySize(quotaUsed));

      printSpaces(2);
      System.out.print("Remaining");
      printKVSeparator();
      if (quotaAllowed != -1) {
        System.out.println(
            FileUtils.byteCountToDisplaySize(quotaAllowed - quotaUsed));
      } else {
        System.out.println("Unknown");
      }
    }
    printNewLines(1);
    return null;
  }
}
