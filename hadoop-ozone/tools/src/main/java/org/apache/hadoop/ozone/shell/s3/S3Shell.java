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
package org.apache.hadoop.ozone.shell.s3;

import java.util.function.Supplier;

import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.shell.Shell;

import picocli.CommandLine.Command;

/**
 * Shell for s3 related operations.
 */
@Command(name = "ozone s3",
    description = "Shell for S3 specific operations",
    subcommands = {
        GetS3SecretHandler.class
    })
public class S3Shell extends Shell {

  @Override
  public void execute(String[] argv) {
    TracingUtil.initTracing("s3shell", createOzoneConfiguration());
    TracingUtil.executeInNewSpan("s3shell",
        (Supplier<Void>) () -> {
          super.execute(argv);
          return null;
        });
  }

  /**
   * Main for the S3Shell Command handling.
   *
   * @param argv - System Args Strings[]
   */
  public static void main(String[] argv) {
    new S3Shell().run(argv);
  }
}
