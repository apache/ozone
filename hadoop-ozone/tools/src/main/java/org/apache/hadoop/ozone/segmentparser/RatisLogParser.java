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
package org.apache.hadoop.ozone.segmentparser;

import io.opentracing.Scope;
import io.opentracing.util.GlobalTracer;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import picocli.CommandLine;


@CommandLine.Command(
    name = "ratislogparser",
    description = "Shell of printing Ratis Log in understandable text",
    subcommands = {
        DatanodeParser.class,
        GenericParser.class,
        //TODO: Add more parsers like for OM and SCM here.
    },
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
/**
 * Parse Ratis Log CLI implementation.
 */
public class RatisLogParser extends GenericCli {

  @Override
  public void execute(String[] argv) {
    TracingUtil.initTracing("logparse", createOzoneConfiguration());
    try (Scope scope = GlobalTracer.get().buildSpan("main").startActive(true)) {
      super.execute(argv);
    }
  }

  public static void main(String[] args) {
    new RatisLogParser().run(args);
  }
}
