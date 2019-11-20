/**
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

package org.apache.hadoop.ozone.recon.api;

import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestTaskTimeService {
  private TaskTimesService taskTimeService;

  @Test
  public void testGetTaskTimes() {
    ReconTaskStatusDao reconTaskStatusDao = mock(ReconTaskStatusDao.class);

    ReconTaskStatus reconTaskStatusRecord = new ReconTaskStatus(
        "Dummy_Task", System.currentTimeMillis(), 0L);
    reconTaskStatusDao.insert(reconTaskStatusRecord);

    List<ReconTaskStatus> resultList = new ArrayList<>();
    resultList.add(reconTaskStatusRecord);

    taskTimeService = mock(TaskTimesService.class);

    when(taskTimeService.getDao()).thenReturn(reconTaskStatusDao);
    when(reconTaskStatusDao.findAll()).thenReturn(resultList);
    when(taskTimeService.getTaskTimes()).thenCallRealMethod();


    Response response = taskTimeService.getTaskTimes();

    List<ReconTaskStatus> responseList = (List<ReconTaskStatus>)
        response.getEntity();

    Assert.assertEquals(1, responseList.size());
    for(ReconTaskStatus r : responseList) {
      Assert.assertEquals(reconTaskStatusRecord.getTaskName(), r.getTaskName());
      Assert.assertEquals(reconTaskStatusRecord.getLastUpdatedTimestamp(),
          r.getLastUpdatedTimestamp());
    }
  }
}
