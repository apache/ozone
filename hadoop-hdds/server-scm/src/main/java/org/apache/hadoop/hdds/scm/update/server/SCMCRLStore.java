/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.hdds.scm.update.server;

import org.apache.hadoop.hdds.scm.update.client.CRLStore;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateServer;
import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class for SCM CRL store.
 */
public class SCMCRLStore implements CRLStore {

  private final CertificateServer certServer;

  public SCMCRLStore(CertificateServer certServer) {
    this.certServer = certServer;
  }

  @Override
  public long getLatestCrlId() {
    return certServer.getLatestCrlId();
  }

  @Override
  public CRLInfo getCRL(long crlId) throws IOException {
    List<Long> crlIdList = new ArrayList<>();
    crlIdList.add(crlId);
    return certServer.getCrls(crlIdList).get(0);
  }
}
