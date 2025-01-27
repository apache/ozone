/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.cli.datanode;

import picocli.CommandLine;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/** Parameter for specifying list of hostnames. */
@CommandLine.Command
public class HostNameParameters {

  @CommandLine.Parameters(description = "One or more host names separated by spaces. " +
      "To read from stdin, specify '-' and supply the host names " +
      "separated by newlines.",
      arity = "1..*",
      paramLabel = "<host name>")
  private List<String> parameters = new ArrayList<>();

  public List<String> getHostNames() {
    List<String> hosts;
    // Whether to read from stdin
    if (parameters.get(0).equals("-")) {
      hosts = new ArrayList<>();
      Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.name());
      while (scanner.hasNextLine()) {
        hosts.add(scanner.nextLine().trim());
      }
    } else {
      hosts = parameters;
    }
    return hosts;
  }

}
