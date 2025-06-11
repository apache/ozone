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

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.makeHttpCall;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.parseInputPath;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printEmptyPathRequest;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printKVSeparator;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printNewLines;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printPathNotFound;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printSpaces;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printWithUnderline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.concurrent.Callable;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.shell.ListLimitOptions;
import org.apache.hadoop.ozone.shell.PrefixFilterOption;
import picocli.CommandLine;

/**
 * Disk Usage Subcommand.
 */
@CommandLine.Command(
    name = "du",
    description = "Get disk usage for a path request.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)

public class DiskUsageSubCommand implements Callable {
  @CommandLine.ParentCommand
  private NSSummaryAdmin parent;

  @CommandLine.Parameters(index = "0", arity = "0..1",
      description = "Non-empty path request without any protocol prefix.")
  private String path;

  @CommandLine.Option(names = {"-f", "--file"},
      description = "List direct files as a sub path.")
  private boolean listFiles;

  @CommandLine.Option(names = {"-r", "--replica"},
      description = "Show disk usage with replication.")
  private boolean withReplica;

  @CommandLine.Option(names = {"-n", "--no-header"},
      description = "Show DU without the header for current path.")
  private boolean noHeader;

  @CommandLine.Mixin
  private ListLimitOptions listOptions;

  @CommandLine.Mixin
  private PrefixFilterOption prefixFilter;

  private static final String ENDPOINT = "/api/v1/namespace/usage";

  // For text alignment
  private static final String SIZE_HEADER = "Size";
  private static final String DU_HEADER = "Disk Usage";
  private static final String PATH_HEADER = "Path Name";
  private static final int SIZE_INDENT = 2;
  private static final int DU_INDENT = 12;
  private static final int PATH_INDENT = 27;

  @Override
  public Void call() throws Exception {
    if (path == null || path.isEmpty()) {
      printEmptyPathRequest();
      return null;
    }
    StringBuilder url = new StringBuilder();
    url.append(parent.getReconWebAddress()).append(ENDPOINT);

    String response = makeHttpCall(url, parseInputPath(path), listFiles,
        withReplica, parent.isHTTPSEnabled(), parent.getOzoneConfig());

    printNewLines(1);
    if (response == null) {
      printNewLines(1);
      return null;
    }

    JsonNode duResponse = JsonUtils.readTree(response);

    if ("PATH_NOT_FOUND".equals(duResponse.path("status").asText(""))) {
      printPathNotFound();
    } else {

      long totalSize = duResponse.path("size").asLong(-1);
      if (!noHeader) {
        printWithUnderline("Path", false);
        printKVSeparator();
        System.out.println(duResponse.path("path").asText(""));

        printWithUnderline("Total Size", false);
        printKVSeparator();
        System.out.println(FileUtils.byteCountToDisplaySize(totalSize));

        if (withReplica) {
          printWithUnderline("Total Disk Usage", false);
          printKVSeparator();
          long du = duResponse.path("sizeWithReplica").asLong(-1);
          System.out.println(FileUtils.byteCountToDisplaySize(du));
        }

        long sizeDirectKey = duResponse.path("sizeDirectKey").asLong(-1);
        if (!listFiles && sizeDirectKey != -1) {
          printWithUnderline("Size of Direct Keys", false);
          printKVSeparator();
          System.out.println(FileUtils.byteCountToDisplaySize(sizeDirectKey));
        }
        printNewLines(1);
      }

      if (duResponse.path("subPathCount").asInt(-1) == 0) {
        if (totalSize == 0) {
          // the object is empty
          System.out.println("The object is empty.\n" +
              "Put more files into it to visualize DU");
        } else {
          System.out.println("There's no immediate " +
              "sub-path under this object.");
          // remind clients if listFiles is not enabled
          if (!listFiles) {
            System.out.println("Add -f as an option to visualize files " +
                "as sub-path, if any.");
          }
        }
      } else {
        printWithUnderline("DU", true);
        printDUHeader();
        int limit = listOptions.getLimit();
        String seekStr = prefixFilter.getPrefix();
        if (seekStr == null) {
          seekStr = "";
        }

        ArrayNode subPaths = (ArrayNode) duResponse.path("subPaths");
        int cnt = 0;
        for (JsonNode subPathDU : subPaths) {
          if (cnt >= limit) {
            break;
          }
          String subPath = subPathDU.path("path").asText("");
          // differentiate key from other types
          if (!subPathDU.path("isKey").asBoolean(false)) {
            subPath += OM_KEY_PREFIX;
          }
          long size = subPathDU.path("size").asLong(-1);
          long sizeWithReplica = subPathDU.path("sizeWithReplica").asLong(-1);
          if (subPath.startsWith(seekStr)) {
            printDURow(subPath, size, sizeWithReplica);
            ++cnt;
          }
        }
      }
    }
    printNewLines(1);
    return null;
  }

  private void printDUHeader() {
    printSpaces(SIZE_INDENT);
    System.out.print(SIZE_HEADER);
    printSpaces(DU_INDENT - SIZE_INDENT - SIZE_HEADER.length());
    if (withReplica) {
      System.out.print(DU_HEADER);
      printSpaces(PATH_INDENT - DU_INDENT - DU_HEADER.length());
      System.out.println(PATH_HEADER);
    } else {
      System.out.println(PATH_HEADER);
    }
  }

  private void printDURow(String subPath, long size, long sizeWithReplica) {
    printSpaces(SIZE_INDENT);
    String dataSize = FileUtils.byteCountToDisplaySize(size);
    System.out.print(dataSize);
    printSpaces(DU_INDENT - SIZE_INDENT - dataSize.length());
    if (sizeWithReplica != -1) {
      String dataSizeWithReplica =
          FileUtils.byteCountToDisplaySize(sizeWithReplica);
      System.out.print(dataSizeWithReplica);
      printSpaces(PATH_INDENT - DU_INDENT - dataSizeWithReplica.length());
    }
    System.out.println(subPath);
  }
}
