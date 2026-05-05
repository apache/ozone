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

package org.apache.hadoop.ozone.shell.token;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Objects;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.Shell;
import org.apache.hadoop.security.token.Token;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Executes getDelegationToken api.
 */
@Command(name = "get",
    description = "get a delegation token.")
public class GetTokenHandler extends Handler {

  @CommandLine.Parameters(arity = "0..1",
      description = Shell.OZONE_URI_DESCRIPTION)
  private String uri;

  @CommandLine.Mixin
  private RenewerOption renewer;

  @CommandLine.Mixin
  private TokenOption tokenFile;

  @Override
  protected OzoneAddress getAddress() throws OzoneClientException {
    return new OzoneAddress(uri);
  }

  @Override
  protected boolean isApplicable() {
    return securityEnabled();
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    Token<OzoneTokenIdentifier> token = client.getObjectStore()
        .getDelegationToken(new Text(renewer.getValue()));
    if (Objects.isNull(token)) {
      err().println("Error: Get delegation token operation failed. " +
          "Check OzoneManager logs for more details.");
    } else {
      out().println("Successfully get token for service " +
          token.getService());
      out().println(token.toString());
      tokenFile.persistToken(token);
    }

    // Set file permission to be readable by only the user who owns the file
    Path tokenFilePath = Paths.get(tokenFile.getTokenFilePath());
    Files.setPosixFilePermissions(tokenFilePath,
        PosixFilePermissions.fromString("rw-------"));
  }
}
