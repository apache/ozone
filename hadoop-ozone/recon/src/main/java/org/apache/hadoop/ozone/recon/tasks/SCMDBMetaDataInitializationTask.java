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

package org.apache.hadoop.ozone.recon.tasks;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.recon.scm.ReconSCMMetadataProcessingTask;
import org.apache.hadoop.ozone.recon.scm.ReconScmMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.jooq.tools.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Class to iterate over the SCM DB metadata events for containers, pipelines, nodes
 * and update recon's corresponding in-memory data using their respective manager
 * and state manager classes.
 */
public class SCMDBMetaDataInitializationTask implements ReconSCMMetadataProcessingTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMDBMetaDataInitializationTask.class);

  private OzoneConfiguration configuration;
  private ReconScmMetadataManager scmMetadataManager;
  private ReconContainerMetadataManager reconContainerMetadataManager;
  private Map<String, SCMMetaDataTableHandler> tableHandlers;
  private Map<HddsProtos.LifeCycleState, Long> containerCountMap;

  @Inject
  public SCMDBMetaDataInitializationTask(ReconScmMetadataManager scmMetadataManager,
                                         ReconContainerMetadataManager reconContainerMetadataManager,
                                         OzoneConfiguration configuration) throws IOException {
    this.scmMetadataManager = scmMetadataManager;
    this.reconContainerMetadataManager = reconContainerMetadataManager;
    this.configuration = configuration;

    containerCountMap = new HashMap<>();

    // Initialize table handlers
    tableHandlers = new HashMap<>();
    tableHandlers.put(ReconScmMetadataManager.CONTAINERS_TABLE,
        new ContainersInfoHandler(reconContainerMetadataManager, scmMetadataManager, containerCountMap));
    tableHandlers.put(ReconScmMetadataManager.PIPELINES_TABLE, new PipelineInfoHandler(scmMetadataManager));
    tableHandlers.put(ReconScmMetadataManager.SEQUENCE_ID_TABLE, new SeqIdGenInfoHandler(scmMetadataManager));
  }

  /**
   * Read container table data and compute stats like DELETED container count,
   * number of blocks per container etc.
   */
  @Override
  public Pair<String, Boolean> reprocess(ReconScmMetadataManager reconScmMetadataManager) throws IOException {
    for (String tableName : getTaskTables()) {
      Table table = scmMetadataManager.getTable(tableName);
      if (table == null) {
        LOG.error("Table " + tableName + " not found in SCM Metadata.");
        return new ImmutablePair<>(getTaskName(), false);
      }
      if (tableHandlers.containsKey(tableName)) {
        tableHandlers.get(tableName).reprocess(reconScmMetadataManager);
      }
    }
    LOG.info("Completed a 'reprocess' run of SCMDBMetaDataInitializationTask.");
    return new ImmutablePair<>(getTaskName(), true);
  }

  @Override
  public String getTaskName() {
    return "SCMDBMetaDataInitializationTask";
  }

  public Collection<String> getTaskTables() throws IOException {
    List<String> taskTables = new ArrayList<>();
    taskTables.add(scmMetadataManager.getContainerTable().getName());
    return taskTables;
  }

  @Override
  public Pair<String, Boolean> process(RocksDBUpdateEventBatch events) {
    Iterator<RocksDBUpdateEvent> eventIterator = events.getIterator();
    int eventCount = 0;
    Collection<String> taskTables = Collections.EMPTY_SET;
    String tableName = StringUtils.EMPTY;
    try {
      taskTables = getTaskTables();

      while (eventIterator.hasNext()) {
        RocksDBUpdateEvent<?, Object> rocksDBUpdateEvent = eventIterator.next();
        tableName = rocksDBUpdateEvent.getTable();
        if (!taskTables.contains(tableName)) {
          continue;
        }
        switch (rocksDBUpdateEvent.getAction()) {
        case PUT:
          handlePutEvent(rocksDBUpdateEvent, tableName);
          break;
        case DELETE:
          handleDeleteEvent(rocksDBUpdateEvent, tableName);
          break;
        case UPDATE:
          handleUpdateEvent(rocksDBUpdateEvent, tableName);
          break;
        default:
          LOG.debug("Skipping DB update event : {}",
              rocksDBUpdateEvent.getAction());
        }
        eventCount++;
      }
    } catch (IOException ioe) {
      LOG.error("Unexpected error while processing SCM MetaData DB event on table - {} from SCM - {}",
          tableName, ioe);
    }
    LOG.info("{} successfully processed {} SCM MetaData DB update event(s).", getTaskName(), eventCount);
    return new ImmutablePair<>(getTaskName(), true);
  }

  private void handleUpdateEvent(RocksDBUpdateEvent<?, Object> event, String tableName) {
    SCMMetaDataTableHandler scmMetaDataTableHandler = tableHandlers.get(tableName);
    if (event.getValue() != null) {
      if (scmMetaDataTableHandler != null) {
        scmMetaDataTableHandler.handleUpdateEvent(event);
      }
    }
  }

  private void handleDeleteEvent(RocksDBUpdateEvent<?, Object> event, String tableName) {
    SCMMetaDataTableHandler scmMetaDataTableHandler = tableHandlers.get(tableName);
    if (event.getValue() != null) {
      if (scmMetaDataTableHandler != null) {
        scmMetaDataTableHandler.handleDeleteEvent(event);
      }
    }
  }

  private void handlePutEvent(RocksDBUpdateEvent<?, Object> event, String tableName) {
    SCMMetaDataTableHandler scmMetaDataTableHandler = tableHandlers.get(tableName);
    if (event.getValue() != null) {
      if (scmMetaDataTableHandler != null) {
        scmMetaDataTableHandler.handlePutEvent(event);
      }
    }
  }

}
