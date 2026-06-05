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

package org.apache.hadoop.ozone.iceberg;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteTablePath;
import org.apache.iceberg.hadoop.HadoopTables;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * CLI to rewrite Iceberg table paths.
 */
@Command(
    name = "rewrite-path",
    description = "Rewrite Iceberg table paths for table migration",
    mixinStandardHelpOptions = true
)
public class RewriteTablePathCommand implements Runnable {

  @Option(
      names = {"-l", "--table-location"},
      required = true,
      description = "The latest metadata.json file path of the table"
  )
  private String tableLocation;

  @Option(
      names = {"-s", "--source-prefix"},
      required = true,
      description = "Source path prefix to replace"
  )
  private String sourcePrefix;

  @Option(
      names = {"-t", "--target-prefix"},
      required = true,
      description = "Target path prefix"
  )
  private String targetPrefix;

  @Option(
      names = {"--staging"},
      description = "Staging location where all the rewritten files will be placed "
          + "(Default is a new directory under the table's current metadata directory.)"
  )
  private String stagingLocation;

  @Option(
      names = {"--start-version"},
      description = "Start version metadata file name (optional, e.g., v1.metadata.json)"
  )
  private String startVersion;

  @Option(
      names = {"--end-version"},
      description = "End version metadata file name (optional, defaults to current)"
  )
  private String endVersion;

  @Option(
      names = {"--threads"},
      description = "Number of threads to use (positive integer). "
          + "If omitted or zero, the default thread count is used."
  )
  private int threads;

  @Override
  public void run() {
    System.out.println("Starting Iceberg table path rewrite");
    System.out.println("Table location: " + tableLocation);
    System.out.println("Source prefix: " + sourcePrefix);
    System.out.println("Target prefix: " + targetPrefix);

    OzoneConfiguration conf = new OzoneConfiguration();

    Table table = null;
    if (tableLocation != null && !tableLocation.isBlank()) {
      HadoopTables tables = new HadoopTables(conf);
      table = tables.load(tableLocation.trim());
      System.out.println("Table loaded: " + table.location());
    }

    RewriteTablePathOzoneAction action;
    if (threads > 0) {
      action = new RewriteTablePathOzoneAction(table, threads);
    } else {
      action = new RewriteTablePathOzoneAction(table);
    }
    System.out.println("Threads: " + action.getThreads());

    RewriteTablePath rewriteAction = action.rewriteLocationPrefix(sourcePrefix, targetPrefix);

    if (stagingLocation != null && !stagingLocation.isBlank()) {
      System.out.println("Staging location: " + stagingLocation);
      rewriteAction.stagingLocation(stagingLocation);
    }

    if (startVersion != null && !startVersion.isBlank()) {
      System.out.println("Start version: " + startVersion);
      rewriteAction.startVersion(startVersion);
    }

    if (endVersion != null && !endVersion.isBlank()) {
      System.out.println("End version: " + endVersion);
      rewriteAction.endVersion(endVersion);
    }

    RewriteTablePath.Result result = rewriteAction.execute();

    System.out.println();
    System.out.println("Rewrite completed successfully");
    System.out.println("  Latest version: " + result.latestVersion());
    System.out.println("  Staging location: " + result.stagingLocation());
    System.out.println();
    System.out.println("Next step: Copy files from source to target using the file list");
    System.out.println("  File list location: " + result.fileListLocation());
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new RewriteTablePathCommand()).execute(args);
    System.exit(exitCode);
  }
}
