/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.api.types;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.OrphanKeyMetaDataProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.OrphanKeysMetaDataSetProto;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Contains Orphan Keys MetaData.
 *
 * For Recon DB table definition.
 */
public class OrphanKeysMetaDataSet {

  private Set<OrphanKeyMetaData> orphanKeyMetaDataSet;

  public OrphanKeysMetaDataSet(
      Set<OrphanKeyMetaData> orphanKeyMetaDataSet) {
    this.orphanKeyMetaDataSet = new HashSet<>(orphanKeyMetaDataSet);
  }

  public Set<OrphanKeyMetaData> asImmutableSet() {
    return Collections.unmodifiableSet(orphanKeyMetaDataSet);
  }

  public Set<OrphanKeyMetaData> getSet() {
    return orphanKeyMetaDataSet;
  }

  public static OrphanKeysMetaDataSet fromProto(
      OrphanKeysMetaDataSetProto proto) {
    Set<OrphanKeyMetaData> orphanKeyMetaDataCollection = new HashSet<>();
    for (OrphanKeyMetaDataProto rhProto : proto.getOrphanKeyMetaDataList()) {
      orphanKeyMetaDataCollection.add(OrphanKeyMetaData.fromProto(rhProto));
    }
    return new OrphanKeysMetaDataSet(orphanKeyMetaDataCollection);
  }

  public OrphanKeysMetaDataSetProto toProto() {
    OrphanKeysMetaDataSetProto.Builder builder =
        OrphanKeysMetaDataSetProto.newBuilder();
    orphanKeyMetaDataSet.stream()
        .map(OrphanKeyMetaData::toProto)
        .forEach(builder::addOrphanKeyMetaData);
    return builder.build();
  }

}
