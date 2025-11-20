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
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo.OperationArgs;
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
import static org.mockito.Mockito.when;
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

  private static OmCompletedRequestInfo buildCompletedRequestInfo(long trxLogIndex, String keyName, OperationArgs opArgs) {
    return new OmCompletedRequestInfo.Builder()
        .setTrxLogIndex(trxLogIndex)
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setKeyName(keyName)
        .setCreationTime(Time.now())
        .setOpArgs(opArgs)
        .build();
  }

  private List<String> captureEventsProducedByOperation(OmCompletedRequestInfo op, int expectEvents) throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("ozone.notify.kafka.topic", "abc");

    List<String> events = new ArrayList<>();

    OMEventListenerKafkaPublisher plugin = new OMEventListenerKafkaPublisher();
    try (MockedConstruction<OMEventListenerKafkaPublisher.KafkaClientWrapper> mockeKafkaClientWrapper =
             mockConstruction(OMEventListenerKafkaPublisher.KafkaClientWrapper.class)) {

      plugin.initialize(conf, pluginContext);
      plugin.handleCompletedRequest(op);

      OMEventListenerKafkaPublisher.KafkaClientWrapper mock = mockeKafkaClientWrapper.constructed().get(0);
      ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
      verify(mock, times(expectEvents)).send(argument.capture());

      events.addAll(argument.getAllValues());
    }

    return events;
  }

  @Test
  public void testCreateKeyRequestProducesS3CreatedEvent() throws InterruptedException, IOException {
    OmCompletedRequestInfo createRequest = buildCompletedRequestInfo(1L, "some/key1",
        new OperationArgs.CreateKeyArgs());

    List<String> events = captureEventsProducedByOperation(createRequest, 1);
    assertThat(events).hasSize(1);

    assertThat(events.get(0))
        .contains(toJsonKeyVal("key", "vol1/bucket1/some/key1"))
        .contains(toJsonKeyVal("type", "CREATE_KEY"));
  }

  @Test
  public void testCreateFileRequestProducesS3CreatedEvent() throws InterruptedException, IOException {
    boolean recursive = false;
    boolean overwrite = true;

    OmCompletedRequestInfo createRequest = buildCompletedRequestInfo(2L, "some/key2",
        new OperationArgs.CreateFileArgs(recursive, overwrite));

    List<String> events = captureEventsProducedByOperation(createRequest, 1);
    assertThat(events).hasSize(1);

    assertThat(events.get(0))
        .contains(toJsonKeyVal("key", "vol1/bucket1/some/key2"))
        .contains(toJsonKeyVal("type", "CREATE_FILE"));
  }

  @Test
  public void testCreateDirectoryRequestProducesS3CreatedEvent() throws InterruptedException, IOException {
    OmCompletedRequestInfo createRequest = buildCompletedRequestInfo(3L, "some/key3",
        new OperationArgs.CreateDirectoryArgs());

    List<String> events = captureEventsProducedByOperation(createRequest, 1);
    assertThat(events).hasSize(1);

    assertThat(events.get(0))
        .contains(toJsonKeyVal("key", "vol1/bucket1/some/key3"))
        .contains(toJsonKeyVal("type", "CREATE_DIRECTORY"));
  }

  @Test
  public void testRenameRequestProducesS3CreateAndDeleteEvents() throws InterruptedException, IOException {
    OmCompletedRequestInfo renameRequest = buildCompletedRequestInfo(4L, "some/key4",
        new OperationArgs.RenameKeyArgs("some/key_RENAMED"));

    List<String> events = captureEventsProducedByOperation(renameRequest, 1);
    assertThat(events).hasSize(1);

    assertThat(events.get(0))
        .contains(toJsonKeyVal("key", "vol1/bucket1/some/key4"))
        .contains(toJsonKeyVal("type", "RENAME_KEY"));
  }
}
