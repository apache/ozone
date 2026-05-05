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

package org.apache.hadoop.ozone.admin.nssummary;

import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.makeHttpCall;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.parseInputPath;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printEmptyPathRequest;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printKVSeparator;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printNewLines;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printPathNotFound;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printWithUnderline;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.server.JsonUtils;
import picocli.CommandLine;

/**
 * Namespace Summary Subcommand.
 */
@CommandLine.Command(
    name = "summary",
    description = "Get entity type and object counts for a path request.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)

public class SummarySubCommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private NSSummaryAdmin parent;

  @CommandLine.Parameters(index = "0", arity = "0..1",
      description = "Non-empty path request without any protocol prefix.")
  private String path;

  private static final String ENDPOINT = "/api/v1/namespace/summary";

  @Override
  public Void call() throws Exception {
    if (path == null || path.isEmpty()) {
      printEmptyPathRequest();
      return null;
    }
    StringBuilder url = new StringBuilder();
    url.append(parent.getReconWebAddress()).append(ENDPOINT);

    printNewLines(1);
    String response = makeHttpCall(url, parseInputPath(path),
        parent.isHTTPSEnabled(), parent.getOzoneConfig());
    if (response == null) {
      printNewLines(1);
      return null;
    }
    JsonNode summaryResponse = JsonUtils.readTree(response);

    if ("PATH_NOT_FOUND".equals(summaryResponse.path("status").asText())) {
      printPathNotFound();
    } else {
      printWithUnderline("Entity Type", false);
      printKVSeparator();
      System.out.println(summaryResponse.get("type"));

      JsonNode countStatsNode = summaryResponse.path("countStats");

      int numVol = countStatsNode.path("numVolume").asInt(-1);
      int numBucket = countStatsNode.path("numBucket").asInt(-1);
      int numDir = countStatsNode.path("numDir").asInt(-1);
      int numKey = countStatsNode.path("numKey").asInt(-1);

      if (numVol != -1) {
        printWithUnderline("Volumes", false);
        printKVSeparator();
        System.out.println(numVol);
      }
      if (numBucket != -1) {
        printWithUnderline("Buckets", false);
        printKVSeparator();
        System.out.println(numBucket);
      }
      if (numDir != -1) {
        printWithUnderline("Directories", false);
        printKVSeparator();
        System.out.println(numDir);
      }
      if (numKey != -1) {
        printWithUnderline("Keys", false);
        printKVSeparator();
        System.out.println(numKey);
      }
    }
    printNewLines(1);
    return null;
  }
}
