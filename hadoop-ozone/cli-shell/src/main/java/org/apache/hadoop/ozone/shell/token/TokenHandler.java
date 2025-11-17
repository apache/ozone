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
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.security.token.Token;
import picocli.CommandLine;

/**
 * Handler for requests with an existing token.
 */
public abstract class TokenHandler extends Handler {

  @CommandLine.Mixin
  private TokenOption tokenFile;
  private Token<OzoneTokenIdentifier> token;

  @Override
  protected boolean isApplicable() {
    return securityEnabled() && tokenFile.exists();
  }

  @Override
  protected OzoneClient createClient(OzoneAddress address)
      throws IOException {
    token = tokenFile.decode();
    return OzoneClientFactory.getOzoneClient(getConf(), token);
  }

  Token<OzoneTokenIdentifier> getToken() {
    return token;
  }
}
