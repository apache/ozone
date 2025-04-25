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
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printEmptyPathRequest;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printNewLines;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printPathNotFound;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printSpaces;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printTypeNA;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printWithUnderline;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.concurrent.Callable;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.server.JsonUtils;
import picocli.CommandLine;

/**
 * File Size Distribution Subcommand.
 */
@CommandLine.Command(
    name = "dist",
    description = "Get file size distribution for a path request.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)

public class FileSizeDistSubCommand implements Callable {
  @CommandLine.ParentCommand
  private NSSummaryAdmin parent;

  @CommandLine.Parameters(index = "0", arity = "0..1",
      description = "Non-empty path request without any protocol prefix.")
  private String path;

  private static final String ENDPOINT = "/api/v1/namespace/dist";

  @Override
  public Void call() throws Exception {
    if (path == null || path.isEmpty()) {
      printEmptyPathRequest();
      return null;
    }
    StringBuilder url = new StringBuilder();
    url.append(parent.getReconWebAddress()).append(ENDPOINT);

    printNewLines(1);
    String response = makeHttpCall(url, path,
        parent.isHTTPSEnabled(), parent.getOzoneConfig());

    if (response == null) {
      printNewLines(1);
      return null;
    }
    JsonNode distResponse = JsonUtils.readTree(response);

    if ("PATH_NOT_FOUND".equals(distResponse.path("status").asText())) {
      printPathNotFound();
    } else if ("TYPE_NOT_APPLICABLE".equals(distResponse.path("status").asText())) {
      printTypeNA("File Size Distribution");
    } else {
      printWithUnderline("File Size Distribution", true);
      JsonNode fileSizeDist = distResponse.path("dist");
      double sum = 0;

      for (int i = 0; i < fileSizeDist.size(); ++i) {
        sum += fileSizeDist.get(i).asDouble();
      }
      if (sum == 0) {
        printSpaces(2);
        System.out.println("The object is empty.\n" +
            "Put more files into it to visualize file size distribution");
        printNewLines(1);
        return null;
      }

      for (int i = 0; i < fileSizeDist.size(); ++i) {
        if (fileSizeDist.get(i).asDouble() == 0) {
          continue;
        }
        String label = convertBinIndexToReadableRange(i);
        printDistRow(label, fileSizeDist.get(i).asDouble(), sum);
      }
    }
    printNewLines(1);
    return null;
  }

  private void printDistRow(String label, double num, double sum) {
    printSpaces(2);
    System.out.print(label);
    System.out.print(" - ");
    System.out.printf("%.1f", (num / sum) * 100);
    System.out.print("% ");
    System.out.println("(" + (int) num + ")");
  }

  private String convertBinIndexToReadableRange(int index) {
    long upperBound = (long) Math.pow(2, (10 + index));
    long lowerBound = (index == 0) ? 0L : upperBound / 2;
    return FileUtils.byteCountToDisplaySize(lowerBound) + " -- " +
        FileUtils.byteCountToDisplaySize(upperBound);
  }
}
