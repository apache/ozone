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

package org.apache.hadoop.hdds.scm.update.client;

import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;

import java.io.IOException;
import java.security.cert.X509CRL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

/**
 * In memory Client CRL store, need to integrate with Client Table.
 */
public class ClientCRLStore implements CRLStore {
  private PriorityQueue<CRLInfo> pendingCrls;
  private List<Long> revokedCerts;
  private long localCrlId;

  public ClientCRLStore() {
    localCrlId = 0;
    revokedCerts = new ArrayList<>();
    pendingCrls = new PriorityQueue<>(
        new Comparator<CRLInfo>() {
          @Override
          public int compare(CRLInfo o1, CRLInfo o2) {
            return o1.getRevocationTime()
                .compareTo(o2.getRevocationTime());
          }
        });
  }

  @Override
  public long getLatestCrlId() {
    return localCrlId;
  }

  public void setLocalCrlId(long crlId) {
    localCrlId = crlId;
  }


  @Override
  public CRLInfo getCRL(long crlId) throws IOException {
    return null;
  }

  public void onRevokeCerts(CRLInfo crl) {
    if (crl.shouldRevokeNow()) {
      revokedCerts.addAll(getRevokedCertIds(crl.getX509CRL()));
    } else {
      pendingCrls.add(crl);
    }
    localCrlId = crl.getCrlSequenceID();
  }

  public List<Long> getRevokedCertIds(X509CRL crl) {
    return Collections.unmodifiableList(crl.getRevokedCertificates().stream()
        .map(cert->cert.getSerialNumber().longValue())
        .collect(Collectors.toList()));
  }

  public CRLInfo getNextPendingCrl() {
    return pendingCrls.peek();
  }

  public void removePendingCrl(CRLInfo crl) {
    pendingCrls.remove(crl);
    revokedCerts.addAll(getRevokedCertIds(crl.getX509CRL()));
  }

  public List<Long> getPendingCrlIds() {
    return new ArrayList<>(pendingCrls)
        .stream().map(crl->crl.getCrlSequenceID())
        .collect(Collectors.toList());
  }

}
