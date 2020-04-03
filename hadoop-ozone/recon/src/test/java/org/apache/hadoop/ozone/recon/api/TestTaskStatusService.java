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

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import org.apache.hadoop.ozone.recon.persistence.AbstractSqlDatabaseTest;
import org.hadoop.ozone.recon.schema.ReconTaskSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;
import org.jooq.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Test for Task Status Service.
 */
public class TestTaskStatusService extends AbstractSqlDatabaseTest {
  private TaskStatusService taskStatusService;
  private Configuration sqlConfiguration;
  private Injector childInjector;

  @Before
  public void setUp() throws SQLException {
    sqlConfiguration = getInjector().getInstance((Configuration.class));
    Injector parentInjector = getInjector();
    childInjector = parentInjector.createChildInjector(new AbstractModule() {
      @Override
      protected void configure() {
        taskStatusService = new TaskStatusService();
        bind(ReconTaskStatusDao.class).
            toInstance(new ReconTaskStatusDao(sqlConfiguration));
        bind(TaskStatusService.class).toInstance(taskStatusService);
      }
    });
    ReconTaskSchemaDefinition schemaDefinition = getInjector().
        getInstance(ReconTaskSchemaDefinition.class);
    schemaDefinition.initializeSchema();
  }

  @Test
  public void testGetTaskTimes() {
    ReconTaskStatusDao reconTaskStatusDao =
        new ReconTaskStatusDao(sqlConfiguration);

    ReconTaskStatus reconTaskStatusRecord = new ReconTaskStatus(
        "Dummy_Task", System.currentTimeMillis(), 0L);
    reconTaskStatusDao.insert(reconTaskStatusRecord);

    List<ReconTaskStatus> resultList = new ArrayList<>();
    resultList.add(reconTaskStatusRecord);

    Response response = taskStatusService.getTaskTimes();

    List<ReconTaskStatus> responseList = (List<ReconTaskStatus>)
        response.getEntity();

    Assert.assertEquals(resultList.size(), responseList.size());
    for(ReconTaskStatus r : responseList) {
      Assert.assertEquals(reconTaskStatusRecord.getTaskName(), r.getTaskName());
      Assert.assertEquals(reconTaskStatusRecord.getLastUpdatedTimestamp(),
          r.getLastUpdatedTimestamp());
    }
  }
}
