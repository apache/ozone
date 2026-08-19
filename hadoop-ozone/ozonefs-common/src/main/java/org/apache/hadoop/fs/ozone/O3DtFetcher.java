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

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;

/**
 * A DT fetcher for Ozone RPC URLs.
 */
public class O3DtFetcher extends O3fsDtFetcher {
  @Override
  public Text getServiceName() {
    return new Text(OzoneConsts.OZONE_RPC_SCHEME);
  }

  @Override
  public Token<?> addDelegationTokens(Configuration conf, Credentials creds,
      String renewer, String url) throws Exception {
    String serviceName = getServiceName().toString();
    if (!url.startsWith(serviceName + "://")) {
      url = serviceName + "://" + url;
    }
    URI uri = URI.create(url);
    if (uri.getAuthority() == null) {
      throw new IllegalArgumentException(
          "OM authority is required in Ozone RPC URL: " + url);
    }
    URI ofsUri = new URI(OzoneConsts.OZONE_OFS_URI_SCHEME,
        uri.getAuthority(), "/", null, null);
    return addDelegationTokens(conf, creds, renewer, ofsUri);
  }
}
