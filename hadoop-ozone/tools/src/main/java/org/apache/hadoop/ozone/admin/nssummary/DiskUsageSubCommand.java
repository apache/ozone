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

import com.google.gson.internal.LinkedTreeMap;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.shell.ListOptions;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.getResponseMap;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.makeHttpCall;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printEmptyPathRequest;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printKVSeparator;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printNewLines;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printPathNotFound;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printSpaces;
import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printWithUnderline;

@CommandLine.Command(
    name = "du",
    description = "Get disk usage for a path request.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)

public class DiskUsageSubCommand implements Callable {
  @CommandLine.ParentCommand
  NSSummaryAdmin parent;

  @CommandLine.Parameters(index = "0", arity = "0..1",
      description = "Non-empty path request without any protocol prefix.")
  private String path;

  @CommandLine.Option(names = {"-f", "--file"},
      description = "List direct files as a sub path.")
  private boolean listFiles;

  @CommandLine.Option(names = {"-r", "--replica"},
      description = "Show disk usage with replication.")
  private boolean withReplica;

  @CommandLine.Mixin
  ListOptions listOptions;

  private static final String ENDPOINT = "/api/v1/namespace/du";

  private String URL = null;

  @Override
  public Void call() throws Exception {
    if (path == null || path.length() == 0) {
      printEmptyPathRequest();
      return null;
    }
    URL = parent.getReconWebAddress() + ENDPOINT;
    String response = makeHttpCall(URL, path, listFiles, withReplica);
    HashMap<String, Object> duResponse = getResponseMap(response);

    printNewLines(1);
    if (!duResponse.get("status").equals("OK")) {
      printPathNotFound();
      printNewLines(1);
      return null;
    }
    printWithUnderline("Path", false);
    printKVSeparator();
    System.out.println(duResponse.get("path"));

    printWithUnderline("Total Size", false);
    printKVSeparator();
    long totalSize = (long)(double)duResponse.get("size");
    System.out.println(FileUtils.byteCountToDisplaySize(totalSize));

    if (withReplica) {
      printWithUnderline("Total Disk Usage", false);
      printKVSeparator();
      long du = (long)(double)duResponse.get("sizeWithReplica");
      System.out.println(FileUtils.byteCountToDisplaySize(du));
    }

    long sizeDirectKey = (long)(double)duResponse.get("sizeDirectKey");
    if (!listFiles && sizeDirectKey != -1) {
      printWithUnderline("Size of Direct Keys", false);
      printKVSeparator();
      System.out.println(FileUtils.byteCountToDisplaySize(sizeDirectKey));
    }

    if ((double)duResponse.get("subPathCount") == 0) {
      if (totalSize == 0) {
        // the object is empty
        System.out.println("The object is empty.\n" +
            "Put more files into it to visualize DU");
      } else {
        System.out.println("There's no immediate sub-path under this object.");
        // remind clients if listFiles is not enabled
        if (!listFiles) {
          System.out.println("Add -f as an option to visualize files as sub-path, if any.");
        }
      }
    } else {
      printNewLines(1);
      printWithUnderline("DU", true);
      printSpaces(2);
      printDUHeader(withReplica);
      int limit = listOptions.getLimit();
      String seekStr = listOptions.getPrefix();
      if (seekStr == null) {
        seekStr = "";
      }

      ArrayList duData = (ArrayList)duResponse.get("subPaths");
      int cnt = 0;
      for (int i = 0; i < duData.size(); ++i) {
        if (cnt >= limit) {
          break;
        }
        LinkedTreeMap subPathDU = (LinkedTreeMap) duData.get(i);
        String subPath = subPathDU.get("path").toString();
        // differentiate key from other types
        if (!(boolean)subPathDU.get("isKey")) {
          subPath += OM_KEY_PREFIX;
        }
        long size = (long)(double)subPathDU.get("size");
        long sizeWithReplica = (long)(double)subPathDU.get("sizeWithReplica");
        if (subPath.startsWith(seekStr)) {
          printSpaces(2);
          printDURow(subPath, size, sizeWithReplica);
          ++cnt;
        }
      }
    }

    printNewLines(1);
    return null;
  }

  private void printDUHeader(boolean withReplica) {
    System.out.print("Size");
    printSpaces(5);
    if (withReplica) {
      System.out.print("Disk Usage");
      printSpaces(5);
    }
    System.out.println("Sub-path");
  }

  private void printDURow(String path, long size, long sizeWithReplica) {
    System.out.print(FileUtils.byteCountToDisplaySize(size));
    printSpaces(5);
    if (sizeWithReplica != -1) {
      System.out.print(FileUtils.byteCountToDisplaySize(sizeWithReplica));
      printSpaces(5);
    }
    System.out.println(path);
  }
}
