/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.interfaces;

import java.util.Map;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.impl.TestHddsDispatcher;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

/**
 * Tests Handler interface.
 */
@Timeout(300)
public class TestHandler {

  private OzoneConfiguration conf;
  private HddsDispatcher dispatcher;
  private ContainerSet containerSet;
  private VolumeSet volumeSet;

  @BeforeEach
  public void setup() throws Exception {
    this.conf = new OzoneConfiguration();
    this.containerSet = Mockito.mock(ContainerSet.class);
    this.volumeSet = Mockito.mock(MutableVolumeSet.class);
    DatanodeDetails datanodeDetails = Mockito.mock(DatanodeDetails.class);
    StateContext context = ContainerTestUtils.getMockContext(
        datanodeDetails, conf);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    Map<ContainerProtos.ContainerType, Handler> handlers = Maps.newHashMap();
    for (ContainerProtos.ContainerType containerType :
        ContainerProtos.ContainerType.values()) {
      handlers.put(containerType,
          Handler.getHandlerForContainerType(
              containerType, conf,
              context.getParent().getDatanodeDetails().getUuidString(),
              containerSet, volumeSet, metrics,
              TestHddsDispatcher.NO_OP_ICR_SENDER));
    }
    this.dispatcher = new HddsDispatcher(
        conf, containerSet, volumeSet, handlers, null, metrics, null);
  }

  @AfterEach
  public void tearDown() {
    ContainerMetrics.remove();
  }

  @Test
  public void testGetKeyValueHandler() throws Exception {
    Handler kvHandler = dispatcher.getHandler(
        ContainerProtos.ContainerType.KeyValueContainer);

    Assertions.assertTrue((kvHandler instanceof KeyValueHandler),
        "getHandlerForContainerType returned incorrect handler");
  }

  @Test
  public void testGetHandlerForInvalidContainerType() {
    // When new ContainerProtos.ContainerType are added, increment the code
    // for invalid enum.
    ContainerProtos.ContainerType invalidContainerType =
        ContainerProtos.ContainerType.forNumber(2);

    Assertions.assertNull(invalidContainerType,
        "New ContainerType detected. Not an invalid containerType");

    Handler dispatcherHandler = dispatcher.getHandler(invalidContainerType);
    Assertions.assertNull(dispatcherHandler,
        "Get Handler for Invalid ContainerType should return null.");
  }
}
