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

package org.apache.hadoop.ozone.om.request.jobworker;

import java.io.IOException;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.jobworker.command.JobworkerJobState;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.jobworker.OMJobworkerJobUpdateResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.JobworkerJobRecord;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateJobworkerJobRequest;

/** Internal compare-and-set of an assignment or result, applied through OM Ratis. */
public class OMJobworkerJobUpdateRequest extends OMClientRequest {
  public OMJobworkerJobUpdateRequest(OMRequest request) {
    super(request);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final OMRequest request = super.preExecute(ozoneManager);
    // Internal callers submit directly to Ratis. Do not expose assignment mutation as a public OM RPC.
    throw new OMException("Jobworker state updates are internal only: " + request.getCmdType(),
        OMException.ResultCodes.ACCESS_DENIED);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager om, ExecutionContext context) {
    OMResponse.Builder response = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    JobworkerJobRecord committed = null;
    try {
      if (!om.getVersionManager().isAllowed(OMLayoutFeature.JOBWORKER_HA)) {
        throw new OMException("Finalize the Jobworker HA upgrade before creating jobs",
            OMException.ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION);
      }
      UpdateJobworkerJobRequest update = getOmRequest().getUpdateJobworkerJobRequest();
      JobworkerJobRecord proposed = update.getRecord();
      JobworkerJobState.validate(proposed);
      Table<String, JobworkerJobRecord> table = om.getMetadataManager().getJobworkerJobTable();
      synchronized (table) {
        JobworkerJobRecord current = table.get(proposed.getJobKey());
        CommandStatus next = CommandStatus.parseFrom(proposed.getStatus());
        if (!next.getOmServiceId().equals(om.getOMServiceId())) {
          throw new IOException("Jobworker status belongs to a different OM service");
        }
        if (current == null) {
          if (update.getExpectedRevision() != 0 || next.getStatus() != CommandStatus.Status.PENDING) {
            throw new IOException("Jobworker job does not exist or is not a new assignment");
          }
        } else {
          if (current.getRevision() != update.getExpectedRevision()
              || !current.getWorkerId().equals(proposed.getWorkerId())
              || !current.getCommand().equals(proposed.getCommand())) {
            throw new IOException("Jobworker assignment changed; reconcile before updating");
          }
          CommandStatus previous = CommandStatus.parseFrom(current.getStatus());
          if (!previous.getOmServiceId().equals(next.getOmServiceId())
              || !JobworkerJobState.canUpdate(previous, next)) {
            throw new IOException("Invalid Jobworker status transition");
          }
        }
        committed = proposed.toBuilder().setRevision(context.getIndex()).build();
        table.addCacheEntry(new CacheKey<>(committed.getJobKey()), CacheValue.get(context.getIndex(), committed));
      }
      response.setJobworkerJobRecord(committed);
    } catch (IOException | IllegalArgumentException e) {
      response = createErrorOMResponse(response, e).toBuilder();
    }
    return new OMJobworkerJobUpdateResponse(response.build(), committed);
  }
}
