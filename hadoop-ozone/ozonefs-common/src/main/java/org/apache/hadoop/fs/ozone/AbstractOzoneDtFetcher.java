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

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.DtFetcher;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base DT fetcher for Ozone URL schemes.
 */
abstract class AbstractOzoneDtFetcher implements DtFetcher {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractOzoneDtFetcher.class);

  private static final String FETCH_FAILED =
      "Fetch ozone delegation token failed";

  @Override
  public boolean isTokenRequired() {
    return UserGroupInformation.isSecurityEnabled();
  }

  @Override
  public Token<?> addDelegationTokens(Configuration conf, Credentials creds,
      String renewer, String url) throws Exception {
    String serviceName = getServiceName().toString();
    if (!url.startsWith(serviceName + "://")) {
      url = serviceName + "://" + url;
    }
    return addDelegationTokens(conf, creds, renewer, URI.create(url));
  }

  protected Token<?> addDelegationTokens(Configuration conf, Credentials creds,
      String renewer, URI uri) throws IOException {
    LOG.debug("addDelegationTokens from {} renewer {}.", uri, renewer);
    FileSystem fs = FileSystem.get(uri, conf);
    Token<?> token = fs.getDelegationToken(renewer);
    if (token == null) {
      LOG.error(FETCH_FAILED);
      throw new IOException(FETCH_FAILED);
    }
    creds.addToken(token.getService(), token);
    return token;
  }
}
