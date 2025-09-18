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
package org.apache.hadoop.ozone.om.eventlistener;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.helpers.OperationInfo;
import org.apache.hadoop.ozone.om.helpers.OperationInfo.OperationArgs;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockedConstruction;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests {@link OMEventListenerPluginManager}.
 */
@ExtendWith(MockitoExtension.class)
public class TestOMEventListenerKafkaPublisher {

  private static final String VOLUME_NAME = "vol1";
  private static final String BUCKET_NAME = "bucket1";

  @Mock
  private OMEventListenerPluginContext pluginContext;

  // helper to create json key/val string for non exhaustive JSON
  // attribute checking
  private static String toJsonKeyVal(String key, String val) {
    return new StringBuilder()
      .append('\"')
      .append(key)
      .append('\"')
      .append(':')
      .append('\"')
      .append(val)
      .append('\"')
      .toString();
  }

  private static OperationInfo buildOperationInfo(long trxLogIndex, String keyName, OperationArgs opArgs) {
    return new OperationInfo.Builder()
        .setTrxLogIndex(trxLogIndex)
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setKeyName(keyName)
        .setCreationTime(Time.now())
        .setOpArgs(opArgs)
        .build();
  }

  private List<String> captureEventsProducedByOperation(OperationInfo op, int expectEvents) throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("ozone.notify.kafka.topic", "abc");

    List<String> events = new ArrayList<>();

    OMEventListenerKafkaPublisher plugin = new OMEventListenerKafkaPublisher();
    try (MockedConstruction<OMEventListenerKafkaPublisher.KafkaClientWrapper> mockeKafkaClientWrapper =
             mockConstruction(OMEventListenerKafkaPublisher.KafkaClientWrapper.class)) {

      plugin.initialize(conf, pluginContext);
      plugin.handleOperation(op);

      OMEventListenerKafkaPublisher.KafkaClientWrapper mock = mockeKafkaClientWrapper.constructed().get(0);
      ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
      verify(mock, times(expectEvents)).send(argument.capture());

      events.addAll(argument.getAllValues());
    }

    return events;
  }

  @Test
  public void testCreateOperationProducesS3CreatedEvent() throws InterruptedException, IOException {
    OperationInfo createOperation = buildOperationInfo(123L, "some/key",
        new OperationArgs.CreateKeyArgs());

    List<String> events = captureEventsProducedByOperation(createOperation, 1);
    assertThat(events).hasSize(1);

    assertThat(events.get(0))
        .contains(toJsonKeyVal("eventName", "ObjectCreated:Put"))
        .contains(toJsonKeyVal("key", "vol1/bucket1/some/key"));
  }

  @Test
  public void testRenameOperationProducesS3CreateAndDeleteEvents() throws InterruptedException, IOException {
    OperationInfo renameOperation = buildOperationInfo(123L, "some/key",
        new OperationArgs.RenameKeyArgs("some/key_RENAMED"));

    List<String> events = captureEventsProducedByOperation(renameOperation, 2);
    assertThat(events).hasSize(2);

    assertThat(events.get(0))
        .contains(toJsonKeyVal("eventName", "ObjectCreated:Put"))
        .contains(toJsonKeyVal("key", "vol1/bucket1/some/key_RENAMED"));

    assertThat(events.get(1))
        .contains(toJsonKeyVal("eventName", "ObjectRemoved:Delete"))
        .contains(toJsonKeyVal("key", "vol1/bucket1/some/key"));
  }
}
