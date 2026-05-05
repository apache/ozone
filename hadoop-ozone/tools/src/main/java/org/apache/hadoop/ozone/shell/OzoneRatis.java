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

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.ratis.shell.cli.sh.RatisShell;
import picocli.CommandLine;

/**
 * Ozone Ratis Command line tool.
 */
@CommandLine.Command(name = "ozone ratis",
        description = "Shell for running Ratis commands",
        versionProvider = HddsVersionProvider.class,
        mixinStandardHelpOptions = true)
public class OzoneRatis extends GenericCli {

  public static void main(String[] argv) throws Exception {
    new OzoneRatis().run(argv);
  }

  @Override
  public int execute(String[] argv) {
    TracingUtil.initTracing("shell", getOzoneConf());
    String spanName = "ozone ratis" + String.join(" ", argv);
    return TracingUtil.executeInNewSpan(spanName, () -> {
      // TODO: When Ozone has RATIS-2155, update this line to use the RatisShell.Builder
      //       in order to setup TLS and other confs.
      final RatisShell shell = new RatisShell(System.out);
      return shell.run(argv);
    });
  }
}
