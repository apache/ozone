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

package org.apache.hadoop.ozone.om.response.upgrade;

import static org.apache.hadoop.ozone.OzoneConsts.NFS_OBJECT_INDEX_PROGRESS_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.NFS_OBJECT_INDEX_READY_KEY;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmNfsObjectInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/** Persists validated bootstrap rows and their progress in the same batch. */
public class OMNfsObjectIndexBootstrapResponse extends OMClientResponse {
  private Map<String, OmNfsObjectInfo> objects;
  private String progress;
  private boolean ready;

  public OMNfsObjectIndexBootstrapResponse(OMResponse response, Map<String, OmNfsObjectInfo> objects,
      String progress, boolean ready) {
    super(response);
    this.objects = new LinkedHashMap<>(objects);
    this.progress = progress;
    this.ready = ready;
  }

  public OMNfsObjectIndexBootstrapResponse(OMResponse response) {
    super(response);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager manager, BatchOperation batch) throws IOException {
    for (Map.Entry<String, OmNfsObjectInfo> object : objects.entrySet()) {
      manager.getNfsObjectTable().putWithBatch(batch, object.getKey(), object.getValue());
    }
    manager.getMetaTable().putWithBatch(batch, NFS_OBJECT_INDEX_PROGRESS_KEY, progress);
    if (ready) {
      manager.getMetaTable().putWithBatch(batch, NFS_OBJECT_INDEX_READY_KEY,
          Integer.toString(OMLayoutFeature.NFS_OBJECT_INDEX.serialize()));
    }
  }
}
