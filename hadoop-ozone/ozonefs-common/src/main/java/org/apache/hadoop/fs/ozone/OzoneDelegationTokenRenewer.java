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

import static org.apache.hadoop.ozone.client.OzoneClientFactory.getOzoneClient;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;

/**
 * Ozone Delegation Token Renewer.
 */
@InterfaceAudience.Private
public class OzoneDelegationTokenRenewer extends TokenRenewer {

  //Ensure that OzoneConfiguration files are loaded before trying to use
  // the renewer.
  static {
    OzoneConfiguration.activate();
  }

  public Text getKind() {
    return OzoneTokenIdentifier.KIND_NAME;
  }

  @Override
  public boolean handleKind(Text kind) {
    return getKind().equals(kind);
  }

  @Override
  public boolean isManaged(Token<?> token) {
    return true;
  }

  @Override
  public long renew(Token<?> token, Configuration conf) throws IOException {
    Token<OzoneTokenIdentifier> ozoneDt =
        (Token<OzoneTokenIdentifier>) token;
    OzoneConfiguration ozoneConf = OzoneConfiguration.of(conf);
    try (OzoneClient ozoneClient = getOzoneClient(ozoneConf, ozoneDt)) {
      return ozoneClient.getObjectStore().renewDelegationToken(ozoneDt);
    }
  }

  @Override
  public void cancel(Token<?> token, Configuration conf)
      throws IOException, InterruptedException {
    Token<OzoneTokenIdentifier> ozoneDt =
        (Token<OzoneTokenIdentifier>) token;
    OzoneConfiguration ozoneConf = OzoneConfiguration.of(conf);
    try (OzoneClient ozoneClient = getOzoneClient(ozoneConf, ozoneDt)) {
      ozoneClient.getObjectStore().cancelDelegationToken(ozoneDt);
    }
  }
}
