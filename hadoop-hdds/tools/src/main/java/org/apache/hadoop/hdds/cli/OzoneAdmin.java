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
package org.apache.hadoop.hdds.cli;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.security.UserGroupInformation;

import picocli.CommandLine;

/**
 * Ozone Admin Command line tool.
 */
@CommandLine.Command(name = "ozone admin",
    hidden = true,
    description = "Developer tools for Ozone Admin operations",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class OzoneAdmin extends GenericCli {

  private OzoneConfiguration ozoneConf;

  private UserGroupInformation user;

  public OzoneAdmin() {
    super(OzoneAdmin.class);
  }

  @VisibleForTesting
  public OzoneAdmin(OzoneConfiguration conf) {
    super(OzoneAdmin.class);
    ozoneConf = conf;
  }

  public OzoneConfiguration getOzoneConf() {
    if (ozoneConf == null) {
      ozoneConf = createOzoneConfiguration();
    }
    return ozoneConf;
  }

  public UserGroupInformation getUser() throws IOException {
    if (user == null) {
      user = UserGroupInformation.getCurrentUser();
    }
    return user;
  }

  /**
   * Main for the Ozone Admin shell Command handling.
   *
   * @param argv - System Args Strings[]
   */
  public static void main(String[] argv) {
    new OzoneAdmin().run(argv);
  }

  @Override
  public int execute(String[] argv) {
    TracingUtil.initTracing("shell", createOzoneConfiguration());
    String spanName = "ozone admin " + String.join(" ", argv);
    return TracingUtil.executeInNewSpan(spanName,
        () -> super.execute(argv));
  }
}
