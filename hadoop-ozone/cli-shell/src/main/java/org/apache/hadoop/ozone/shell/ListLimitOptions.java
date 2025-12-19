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

/** Options for limiting the size of lists.  Use with {@link CommandLine.Mixin}. */
public class ListLimitOptions {

  @CommandLine.ArgGroup
  private ExclusiveGroup group = new ExclusiveGroup();

  public boolean isAll() {
    return group.all;
  }

  public int getLimit() {
    if (group.all) {
      return Integer.MAX_VALUE;
    }
    if (group.limit < 1) {
      throw new IllegalArgumentException(
          "List length should be a positive number");
    }

    return group.limit;
  }

  static class ExclusiveGroup {
    @CommandLine.Option(names = {"--length", "-l"},
        description = "Maximum number of items to list",
        defaultValue = "100",
        showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    private int limit;

    @CommandLine.Option(names = {"--all", "-a"},
        description = "List all results (without pagination limit)",
        defaultValue = "false")
    private boolean all;
  }
}
