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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.ozone.recon.api.types.ReconTaskStatusResponse;
import org.apache.hadoop.ozone.recon.api.types.ReconTaskStatusStat;
import org.apache.hadoop.ozone.recon.metrics.ReconTaskStatusCounter;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.ws.rs.core.Response;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Test for Task Status Service.
 */
public class TestTaskStatusService extends AbstractReconSqlDBTest {

  @TempDir
  private Path temporaryFolder;
  private TaskStatusService taskStatusService;

  public TestTaskStatusService() {
    super();
  }

  @BeforeEach
  public void setUp() throws Exception {
    ReconTaskStatusCounter reconTaskStatusCounterMock = mock(ReconTaskStatusCounter.class);
    when(reconTaskStatusCounterMock.getTaskStatsFor(anyString())).thenReturn(new ReconTaskStatusStat(
        new AtomicInteger(10), new AtomicInteger(2)
    ));
    taskStatusService = new TaskStatusService(getDao(ReconTaskStatusDao.class), reconTaskStatusCounterMock);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, -1})
  public void testTaskTableValues(int lastTaskRunStatus) {
    ReconTaskStatusDao reconTaskStatusDao = getDao(ReconTaskStatusDao.class);

    ReconTaskStatus reconTaskStatusRecord = new ReconTaskStatus(
        "Dummy_Task", System.currentTimeMillis(), 0L, lastTaskRunStatus, 0);
    reconTaskStatusDao.insert(reconTaskStatusRecord);

    List<ReconTaskStatus> resultList = new ArrayList<>();
    resultList.add(reconTaskStatusRecord);

    Response response = taskStatusService.getTaskStats();

    List<ReconTaskStatusResponse> responseList = (List<ReconTaskStatusResponse>)
        response.getEntity();

    assertEquals(resultList.size(), responseList.size());
    for (ReconTaskStatusResponse r : responseList) {
      assertEquals(reconTaskStatusRecord.getTaskName(), r.getTaskName());
      assertEquals(reconTaskStatusRecord.getLastUpdatedTimestamp(),
          r.getLastUpdatedTimestamp());
      assertEquals(reconTaskStatusRecord.getLastTaskRunStatus(), r.getLastTaskRunStatus());
      assertEquals(reconTaskStatusRecord.getIsCurrentTaskRunning(), r.getIsTaskCurrentlyRunning());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTaskCounts() {
    ReconTaskStatusDao reconTaskStatusDao = getDao(ReconTaskStatusDao.class);
    String taskName = "DummyTask_" + System.currentTimeMillis();
    ReconTaskStatus reconTaskStatusRecord = new ReconTaskStatus(
        taskName, System.currentTimeMillis(), 0L, 0, 0);
    reconTaskStatusDao.insert(reconTaskStatusRecord);

    Response response = taskStatusService.getTaskStats();

    List<ReconTaskStatusResponse> tasks = (List<ReconTaskStatusResponse>) response.getEntity();
    assertEquals(1, tasks.size());
    assertEquals(taskName, tasks.get(0).getTaskName());
    assertEquals(10, tasks.get(0).getSuccessCount());
    assertEquals(2, tasks.get(0).getFailureCount());
  }
}
