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

/** Options to provide pagination of lists.  Use with {@link CommandLine.Mixin}. */
public class ListPaginationOptions {

  @CommandLine.Mixin
  private ListLimitOptions limitOptions;

  @CommandLine.Option(names = {"--start", "-s"},
      description = "The item to start the listing from.\n" +
          "This will be excluded from the result.")
  private String startItem;

  public int getLimit() {
    return limitOptions.getLimit();
  }

  public boolean isAll() {
    return limitOptions.isAll();
  }

  public String getStartItem() {
    return startItem;
  }

}
