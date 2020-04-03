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

package org.apache.hadoop.ozone.web.ozShell.token;

import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.OzoneAddress;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.io.IOException;

/**
 * Executes renewDelegationToken api.
 */
@Command(name = "renew",
    description = "renew a delegation token.")
public class RenewTokenHandler extends Handler {

  @CommandLine.Mixin
  private TokenOption tokenFile;

  @Override
  protected OzoneAddress getAddress() throws OzoneClientException {
    return new OzoneAddress();
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException, OzoneClientException {

    if (securityEnabled("token renew") && tokenFile.exists()) {
      long expiryTime = client.getObjectStore()
          .renewDelegationToken(tokenFile.decode());

      out().printf("Token renewed successfully, expiry time: %s",
          expiryTime);
    }
  }
}
