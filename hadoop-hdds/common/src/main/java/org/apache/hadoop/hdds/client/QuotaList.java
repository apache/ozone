/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.ozone.OzoneConsts.GB;
import static org.apache.hadoop.ozone.OzoneConsts.KB;
import static org.apache.hadoop.ozone.OzoneConsts.MB;
import static org.apache.hadoop.ozone.OzoneConsts.TB;

/**
 *This class contains arraylist for storage constant used in OzoneQuota.
 */
public class QuotaList {
  private final ArrayList<String> ozoneQuota;
  private final ArrayList<OzoneQuota.Units> unitQuota;
  private final ArrayList<Long> sizeQuota;

  public QuotaList() {
    ozoneQuota = new ArrayList<>();
    unitQuota = new ArrayList<>();
    sizeQuota = new ArrayList<>();

    // Setting QuotaList parameters from large to small.
    addQuotaList(OzoneQuota.OZONE_QUOTA_TB, OzoneQuota.Units.TB, TB);
    addQuotaList(OzoneQuota.OZONE_QUOTA_GB, OzoneQuota.Units.GB, GB);
    addQuotaList(OzoneQuota.OZONE_QUOTA_MB, OzoneQuota.Units.MB, MB);
    addQuotaList(OzoneQuota.OZONE_QUOTA_KB, OzoneQuota.Units.KB, KB);
    addQuotaList(OzoneQuota.OZONE_QUOTA_B, OzoneQuota.Units.B, 1L);
  }

  private void addQuotaList(
      String oQuota, OzoneQuota.Units uQuota, Long sQuota) {
    ozoneQuota.add(oQuota);
    unitQuota.add(uQuota);
    sizeQuota.add(sQuota);
  }

  public List<String> getOzoneQuotaArray() {
    return Collections.unmodifiableList(this.ozoneQuota);
  }

  public List<Long> getSizeQuotaArray() {
    return Collections.unmodifiableList(this.sizeQuota);
  }

  public List<OzoneQuota.Units> getUnitQuotaArray() {
    return Collections.unmodifiableList(this.unitQuota);
  }

  public OzoneQuota.Units getUnits(String oQuota) {
    return unitQuota.get(ozoneQuota.indexOf(oQuota));
  }

  public Long getQuotaSize(OzoneQuota.Units uQuota) {
    return sizeQuota.get(unitQuota.indexOf(uQuota));
  }

  public OzoneQuota.Units getQuotaUnit(Long sQuota) {
    return unitQuota.get(sizeQuota.indexOf(sQuota));
  }

}
