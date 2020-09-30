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
package org.apache.hadoop.ozone.shell;

import picocli.CommandLine;

/**
 * Common options for 'list' commands.
 */
public class ListOptions {

  @CommandLine.Option(names = {"--length", "-l"},
      description = "Maximum number of items to list",
      defaultValue = "100",
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private int limit;

  @CommandLine.Option(names = {"--start", "-s"},
      description = "The item to start the listing from.\n" +
          "This will be excluded from the result.")
  private String startItem;

  @CommandLine.Option(names = {"--prefix", "-p"},
      description = "Prefix to filter the items")
  private String prefix;

  public int getLimit() {
    if (limit < 1) {
      throw new IllegalArgumentException(
          "List length should be a positive number");
    }

    return limit;
  }

  public String getStartItem() {
    return startItem;
  }

  public String getPrefix() {
    return prefix;
  }
}
