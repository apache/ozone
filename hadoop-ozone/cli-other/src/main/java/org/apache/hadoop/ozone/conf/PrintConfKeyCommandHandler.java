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

package org.apache.hadoop.ozone.conf;

import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

/**
 * Handler for ozone getconf confKey [key].
 */
@Command(name = "confKey",
    aliases = {"-confKey"},
    description = "gets a specific key from the configuration",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class PrintConfKeyCommandHandler implements Callable<Void> {

  @Parameters(arity = "1..1", description = "configuration key")
  private String confKey;

  @ParentCommand
  private OzoneGetConf tool;

  @Override
  public Void call() throws Exception {
    String value = tool.getConf().getTrimmed(confKey);
    if (value != null) {
      tool.printOut(value);
    } else {
      throw new IllegalArgumentException(
          "Configuration " + confKey + " is missing.");
    }
    return null;
  }
}
