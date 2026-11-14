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

package org.apache.hadoop.fs.ozone;

/**
 * Span attribute keys used by the {@code ozone fs} CLI.
 */
public final class FsShellTracingAttributes {

  /** FsShell command that was run, for example {@code ozone fs -ls}. */
  public static final String CLI_COMMAND = "cli.command";

  /** Arguments of the command, such as paths, separated by spaces. */
  public static final String CLI_ARGS = "cli.args";

  /** Prefix of a CLI option attribute, followed by the option, for example {@code cli.option.-R}. */
  public static final String CLI_OPTION_PREFIX = "cli.option.";

  private FsShellTracingAttributes() {
  }
}
