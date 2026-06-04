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

package org.apache.hadoop.ozone.om.eventlistener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.OzoneIllegalArgumentException;
import org.apache.hadoop.ozone.om.eventlistener.s3.S3EventNotification;
import org.apache.hadoop.ozone.om.eventlistener.s3.S3EventNotification.OzoneEventDataKey;
import org.apache.hadoop.ozone.om.eventlistener.s3.S3EventNotification.S3EventNotificationRecord;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo.OperationArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests {@link OMEventListenerKafkaPublisher}.
 */
@ExtendWith(MockitoExtension.class)
public class TestOMEventListenerKafkaPublisher {

  private static final String VOLUME_NAME = "vol1";
  private static final String BUCKET_NAME = "bucket1";

  @Mock
  private OMEventListenerPluginContext pluginContext;

  private static OmCompletedRequestInfo buildCompletedRequestInfo(
          long trxLogIndex, Type cmdType, String keyName, OperationArgs opArgs) {

    return new OmCompletedRequestInfo.Builder()
        .setTrxLogIndex(trxLogIndex)
        .setCmdType(cmdType)
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setKeyName(keyName)
        .setCreationTime(Time.now())
        .setOpArgs(opArgs)
        .build();
  }

  private List<String> captureEventsProducedByOperation(OmCompletedRequestInfo op, int expectEvents)
          throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("ozone.om.plugin.kafka.topic", "abc");

    List<String> events = new ArrayList<>();

    OMEventListenerKafkaPublisher plugin = new OMEventListenerKafkaPublisher();
    try (MockedConstruction<OMEventListenerKafkaPublisher.KafkaClientWrapper> mockedKafkaClientWrapper =
             mockConstruction(OMEventListenerKafkaPublisher.KafkaClientWrapper.class)) {

      plugin.initialize(conf, pluginContext);
      plugin.handleCompletedRequest(op);

      OMEventListenerKafkaPublisher.KafkaClientWrapper mock = mockedKafkaClientWrapper.constructed().get(0);
      ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
      verify(mock, times(expectEvents)).send(argument.capture());

      events.addAll(argument.getAllValues());
    }

    return events;
  }

  private S3EventNotificationRecord getFirstRecord(List<String> events) throws IOException {
    assertThat(events).hasSize(1);
    S3EventNotification notification = JsonUtils.getDefaultMapper()
        .readValue(events.get(0), S3EventNotification.class);
    assertThat(notification.getRecords()).hasSize(1);
    return notification.getRecords().get(0);
  }

  @Test
  public void testCreateKeyRequestProducesS3CreatedEvent() throws InterruptedException, IOException {
    OmCompletedRequestInfo createRequest = buildCompletedRequestInfo(1L, Type.CreateKey, "some/key1",
        new OperationArgs.NoArgs());

    List<String> events = captureEventsProducedByOperation(createRequest, 1);
    S3EventNotificationRecord record = getFirstRecord(events);

    assertThat(record.getEventName()).isEqualTo("ObjectCreated:Put");
    assertThat(record.getS3().getObject().getKey()).isEqualTo("vol1/bucket1/some/key1");

    Map<String, Object> expectedEventData = new HashMap<>();
    expectedEventData.put(OzoneEventDataKey.OP_TYPE.toString(), "CreateKey");
    expectedEventData.put(OzoneEventDataKey.TRX_LOG_INDEX.toString(), 1);

    assertThat(record.getOzoneEventData()).isEqualTo(expectedEventData);
  }

  @Test
  public void testCommitKeyRequestProducesS3CreatedEvent() throws InterruptedException, IOException {
    OmCompletedRequestInfo commitRequest = buildCompletedRequestInfo(8L, Type.CommitKey, "some/key1_commit",
        new OperationArgs.NoArgs());

    List<String> events = captureEventsProducedByOperation(commitRequest, 1);
    S3EventNotificationRecord record = getFirstRecord(events);

    assertThat(record.getEventName()).isEqualTo("ObjectCreated:Put");
    assertThat(record.getS3().getObject().getKey()).isEqualTo("vol1/bucket1/some/key1_commit");

    Map<String, Object> expectedEventData = new HashMap<>();
    expectedEventData.put(OzoneEventDataKey.OP_TYPE.toString(), "CommitKey");
    expectedEventData.put(OzoneEventDataKey.TRX_LOG_INDEX.toString(), 8);

    assertThat(record.getOzoneEventData()).isEqualTo(expectedEventData);
  }

  @Test
  public void testCreateFileRequestProducesS3CreatedEvent() throws InterruptedException, IOException {
    boolean recursive = false;
    boolean overwrite = true;

    OmCompletedRequestInfo createRequest = buildCompletedRequestInfo(2L, Type.CreateFile, "some/key2",
        new OperationArgs.CreateFileArgs(recursive, overwrite));

    List<String> events = captureEventsProducedByOperation(createRequest, 1);
    S3EventNotificationRecord record = getFirstRecord(events);

    assertThat(record.getEventName()).isEqualTo("ObjectCreated:Put");
    assertThat(record.getS3().getObject().getKey()).isEqualTo("vol1/bucket1/some/key2");

    Map<String, Object> expectedEventData = new HashMap<>();
    expectedEventData.put(OzoneEventDataKey.IS_DIRECTORY.toString(), false);
    expectedEventData.put(OzoneEventDataKey.IS_RECURSIVE.toString(), false);
    expectedEventData.put(OzoneEventDataKey.IS_OVERWRITE.toString(), true);
    expectedEventData.put(OzoneEventDataKey.OP_TYPE.toString(), "CreateFile");
    expectedEventData.put(OzoneEventDataKey.TRX_LOG_INDEX.toString(), 2);

    assertThat(record.getOzoneEventData()).isEqualTo(expectedEventData);
  }

  @Test
  public void testCreateDirectoryRequestProducesS3CreatedEvent() throws InterruptedException, IOException {
    OmCompletedRequestInfo createRequest = buildCompletedRequestInfo(3L, Type.CreateDirectory, "some/key3",
        new OperationArgs.NoArgs());

    List<String> events = captureEventsProducedByOperation(createRequest, 1);
    S3EventNotificationRecord record = getFirstRecord(events);

    assertThat(record.getEventName()).isEqualTo("ObjectCreated:Put");
    assertThat(record.getS3().getObject().getKey()).isEqualTo("vol1/bucket1/some/key3");

    Map<String, Object> expectedEventData = new HashMap<>();
    expectedEventData.put(OzoneEventDataKey.IS_DIRECTORY.toString(), true);
    expectedEventData.put(OzoneEventDataKey.OP_TYPE.toString(), "CreateDirectory");
    expectedEventData.put(OzoneEventDataKey.TRX_LOG_INDEX.toString(), 3);

    assertThat(record.getOzoneEventData()).isEqualTo(expectedEventData);
  }

  @Test
  public void testRenameRequestProducesS3RenamedEvent() throws InterruptedException, IOException {
    OmCompletedRequestInfo renameRequest = buildCompletedRequestInfo(4L, Type.RenameKey, "some/key4",
        new OperationArgs.RenameKeyArgs("some/key_RENAMED"));

    List<String> events = captureEventsProducedByOperation(renameRequest, 1);
    S3EventNotificationRecord record = getFirstRecord(events);

    assertThat(record.getEventName()).isEqualTo("ObjectRenamed:Rename");
    assertThat(record.getS3().getObject().getKey()).isEqualTo("vol1/bucket1/some/key_RENAMED");

    Map<String, Object> expectedEventData = new HashMap<>();
    expectedEventData.put(OzoneEventDataKey.RENAME_FROM_KEY.toString(), "vol1/bucket1/some/key4");
    expectedEventData.put(OzoneEventDataKey.TRX_LOG_INDEX.toString(), 4);

    assertThat(record.getOzoneEventData()).isEqualTo(expectedEventData);
  }

  @Test
  public void testCreateVolumeRequestProducesOzoneVolumeCreatedEvent() throws IOException {
    OmCompletedRequestInfo createRequest = buildCompletedRequestInfo(6L, Type.CreateVolume, null,
        new OperationArgs.NoArgs());
    // override volume/bucket to simulate volume-only op
    createRequest = new OmCompletedRequestInfo.Builder()
        .setTrxLogIndex(6L)
        .setCmdType(Type.CreateVolume)
        .setVolumeName(VOLUME_NAME)
        .setBucketName(null)
        .setKeyName(null)
        .setCreationTime(Time.now())
        .setOpArgs(new OperationArgs.NoArgs())
        .build();

    List<String> events = captureEventsProducedByOperation(createRequest, 1);
    S3EventNotificationRecord record = getFirstRecord(events);

    assertThat(record.getEventName()).isEqualTo("OzoneVolumeCreated:Put");
    assertThat(record.getS3().getBucket().getName()).isNull();
    assertThat(record.getS3().getObject().getKey()).isEqualTo(VOLUME_NAME);

    Map<String, Object> expectedEventData = new HashMap<>();
    expectedEventData.put(OzoneEventDataKey.TRX_LOG_INDEX.toString(), 6);

    assertThat(record.getOzoneEventData()).isEqualTo(expectedEventData);
  }

  @Test
  public void testCreateBucketRequestProducesOzoneBucketCreatedEvent() throws IOException {
    OmCompletedRequestInfo createRequest = new OmCompletedRequestInfo.Builder()
        .setTrxLogIndex(7L)
        .setCmdType(Type.CreateBucket)
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setKeyName(null)
        .setCreationTime(Time.now())
        .setOpArgs(new OperationArgs.NoArgs())
        .build();

    List<String> events = captureEventsProducedByOperation(createRequest, 1);
    S3EventNotificationRecord record = getFirstRecord(events);

    assertThat(record.getEventName()).isEqualTo("OzoneBucketCreated:Put");
    assertThat(record.getS3().getBucket().getName()).isEqualTo(BUCKET_NAME);
    assertThat(record.getS3().getObject().getKey()).isEqualTo(VOLUME_NAME + "/" + BUCKET_NAME);

    Map<String, Object> expectedEventData = new HashMap<>();
    expectedEventData.put(OzoneEventDataKey.TRX_LOG_INDEX.toString(), 7);

    assertThat(record.getOzoneEventData()).isEqualTo(expectedEventData);
  }

  @Test
  public void testEventDateFormatIsIso8601() throws IOException {
    OmCompletedRequestInfo createRequest = buildCompletedRequestInfo(5L, Type.CreateKey, "date/test",
        new OperationArgs.NoArgs());

    List<String> events = captureEventsProducedByOperation(createRequest, 1);
    String rawJson = events.get(0);

    // Parse raw JSON to extract the eventTime string exactly as it appears in the message
    JsonNode root = JsonUtils.readTree(rawJson);
    String eventTimeStr = root.path("Records").get(0).path("eventTime").asText();

    // Validate that it matches ISO-8601 offset format (e.g., 2026-05-20T13:45:00Z or +01:00)
    // This ensures our DateTimeJsonSerializer is working as expected.
    assertThat(eventTimeStr).matches("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*");

    // Also ensure it is actually parsable by the standard Java 8 API
    java.time.OffsetDateTime.parse(eventTimeStr);
  }

  @Test
  public void testInitializeFailsWhenStrategyInstantiationThrows() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setClass("ozone.om.plugin.kafka.notification.strategy",
        ThrowingStrategy.class, OMEventListenerNotificationStrategy.class);

    OMEventListenerKafkaPublisher plugin = new OMEventListenerKafkaPublisher();
    try (MockedConstruction<OMEventListenerKafkaPublisher.KafkaClientWrapper> mockedKafkaClientWrapper =
             mockConstruction(OMEventListenerKafkaPublisher.KafkaClientWrapper.class)) {

      org.junit.jupiter.api.Assertions.assertThrows(OzoneIllegalArgumentException.class, () -> {
        plugin.initialize(conf, pluginContext);
      });
    }
  }

  /**
   * A mock strategy that throws an exception during instantiation for testing.
   */
  public static class ThrowingStrategy implements OMEventListenerNotificationStrategy {
    public ThrowingStrategy() {
      throw new RuntimeException("Simulated instantiation failure");
    }

    @Override
    public List<String> determineEventsForOperation(OmCompletedRequestInfo completedRequestInfo) {
      return null;
    }
  }
}
