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

package org.apache.hadoop.hdds.security.x509.crl;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.List;

/**
 * Class that contains the CRL status.
 */
public class CRLStatus {
  private final long receivedCRLId;
  private final List<Long> pendingCRLIds;

  /**
   * Constructs a CRL Status.
   * @param receivedCRLId The last received CRL id.
   * @param pendingCRLIds A list of CRL Ids that are pending processing.
   */
  public CRLStatus(long receivedCRLId, List<Long> pendingCRLIds) {
    this.receivedCRLId = receivedCRLId;
    this.pendingCRLIds = pendingCRLIds;
  }


  public long getReceivedCRLId() {
    return receivedCRLId;
  }

  public List<Long> getPendingCRLIds() {
    return pendingCRLIds;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof CRLStatus)) {
      return false;
    }

    CRLStatus that = (CRLStatus) obj;

    if (this.receivedCRLId != that.getReceivedCRLId()) {
      return false;
    }
    if (this.pendingCRLIds.size() != that.getPendingCRLIds().size()) {
      return false;
    }

    return CollectionUtils.isEqualCollection(this.pendingCRLIds,
        that.getPendingCRLIds());
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(81, 145)
        .append(receivedCRLId)
        .append(pendingCRLIds)
        .toHashCode();
  }

  @Override
  public String toString() {
    return "CRLStatus{" +
        ", receivedCRLId=" + receivedCRLId +
        ", pendingCRLIds=" + StringUtils.join(pendingCRLIds, ",") +
        '}';
  }
}
