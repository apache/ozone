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

package org.apache.hadoop.ozone.om.eventlistener.s3;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.eventlistener.OMEventListenerNotificationStrategy;
import org.apache.hadoop.ozone.om.eventlistener.s3.S3EventNotification.OzoneEventDataKey;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a notification strategy to generate events according to S3
 * notification semantics.
 */
public class S3EventNotificationStrategy implements OMEventListenerNotificationStrategy {
  public static final Logger LOG = LoggerFactory.getLogger(S3EventNotificationStrategy.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public List<String> determineEventsForOperation(OmCompletedRequestInfo requestInfo) {

    switch (requestInfo.getCmdType()) {
    case CreateVolume:
      return Collections.singletonList(createS3Event("OzoneVolumeCreated:Put",
              requestInfo.getVolumeName(),
              null,
              null,
              Collections.emptyMap())
      );
    case DeleteVolume:
      return Collections.singletonList(createS3Event("OzoneVolumeRemoved:Delete",
              requestInfo.getVolumeName(),
              null,
              null,
              Collections.emptyMap())
      );
    case CreateBucket:
      return Collections.singletonList(createS3Event("OzoneBucketCreated:Put",
              requestInfo.getVolumeName(),
              requestInfo.getBucketName(),
              null,
              Collections.emptyMap())
      );
    case DeleteBucket:
      return Collections.singletonList(createS3Event("OzoneBucketRemoved:Delete",
              requestInfo.getVolumeName(),
              requestInfo.getBucketName(),
              null,
              Collections.emptyMap())
      );
    case CreateKey:
    case CommitKey:
      return Collections.singletonList(createS3Event("ObjectCreated:Put",
              requestInfo.getVolumeName(),
              requestInfo.getBucketName(),
              requestInfo.getKeyName(),
              Collections.emptyMap())
      );
    case CreateFile:
      OmCompletedRequestInfo.OperationArgs.CreateFileArgs createFileArgs
          = (OmCompletedRequestInfo.OperationArgs.CreateFileArgs) requestInfo.getOpArgs();

      // XXX: ozoneEventData is an Ozone extension. It is unclear if this
      // schema makes sense but the general S3 schema is somewhat
      // freeform.  These arguments are more informational than
      // required so it is unclear as to their necessity.
      Map<String, Object> createFileEventData = new HashMap<>();
      createFileEventData.put(OzoneEventDataKey.IS_DIRECTORY.toString(), false);
      createFileEventData.put(OzoneEventDataKey.IS_RECURSIVE.toString(), createFileArgs.isRecursive());
      createFileEventData.put(OzoneEventDataKey.IS_OVERWRITE.toString(), createFileArgs.isOverwrite());

      return Collections.singletonList(createS3Event("ObjectCreated:Put",
              requestInfo.getVolumeName(),
              requestInfo.getBucketName(),
              requestInfo.getKeyName(),
              createFileEventData)
      );
    case CreateDirectory:
      // XXX: ozoneEventData is an Ozone extension. It is unclear if this
      // schema makes sense but the general S3 schema is somewhat
      // freeform.  These arguments are more informational than
      // required so it is unclear as to their necessity.
      Map<String, Object> createEventData = new HashMap<>();
      createEventData.put(OzoneEventDataKey.IS_DIRECTORY.toString(), true);

      return Collections.singletonList(createS3Event("ObjectCreated:Put",
              requestInfo.getVolumeName(),
              requestInfo.getBucketName(),
              requestInfo.getKeyName(),
              createEventData)
      );
    case DeleteKey:
      return Collections.singletonList(createS3Event("ObjectRemoved:Delete",
              requestInfo.getVolumeName(),
              requestInfo.getBucketName(),
              requestInfo.getKeyName(),
              Collections.emptyMap())
      );
    case RenameKey:
      OmCompletedRequestInfo.OperationArgs.RenameKeyArgs renameKeyArgs
          = (OmCompletedRequestInfo.OperationArgs.RenameKeyArgs) requestInfo.getOpArgs();

      String renameFromKey = S3OzoneEventKeyFormatter.getOzoneKey(requestInfo.getVolumeName(),
                                                      requestInfo.getBucketName(),
                                                      requestInfo.getKeyName());

      // XXX: it would be good to be able to convey that this was a
      // file vs directory rename
      Map<String, Object> ozoneEventData = new HashMap<>();
      ozoneEventData.put(OzoneEventDataKey.RENAME_FROM_KEY.toString(), renameFromKey);

      // NOTE: ObjectRenamed:Rename is an Ozone extension as is the
      // ozoneEventData map in the S3 event schema.
      return Collections.singletonList(createS3Event("ObjectRenamed:Rename",
              requestInfo.getVolumeName(),
              requestInfo.getBucketName(),
              renameKeyArgs.getToKeyName(),
              ozoneEventData)
      );
    default:
      LOG.debug("No events for operation {} on {}",
           requestInfo.getCmdType(),
           requestInfo.getKeyName());
      return Collections.emptyList();
    }
  }

  static String createS3Event(String eventName,
                              String volumeName,
                              String bucketName,
                              String keyName,
                              Map<String, Object> ozoneEventData) {
    try {
      String objectKey = S3OzoneEventKeyFormatter.getOzoneKey(volumeName, bucketName, keyName);
      String bucketArn = (bucketName == null)
          ? "arn:aws:s3:::" + volumeName
          : "arn:aws:s3:::" + volumeName + "." + bucketName;
      Instant eventTime = Instant.now();
      String etag = UUID.randomUUID().toString();

      S3EventNotification event = new S3EventNotificationBuilder(objectKey, bucketName, bucketArn,
                                                                 eventName, eventTime, etag)
          .addAllEventData(ozoneEventData)
          .build();

      return MAPPER.writer().writeValueAsString(event);
    } catch (Exception ex) {
      LOG.error("Failed to create S3 event for {} on {}/{}", eventName, volumeName, bucketName, ex);
      return null;
    }
  }

  /**
   * Formats the Ozone key for S3 events.
   *
   * NOTE: This differs from OMMetadataManager#getOzoneKey in that it does NOT
   * include the leading slash, which is standard for S3 notification keys.
   */
  private static class S3OzoneEventKeyFormatter {
    public static String getOzoneKey(String volume, String bucket, String key) {
      StringBuilder builder = new StringBuilder();
      builder.append(volume);
      if (StringUtils.isNotBlank(bucket)) {
        builder.append(OM_KEY_PREFIX).append(bucket);
        if (StringUtils.isNotBlank(key)) {
          builder.append(OM_KEY_PREFIX);
          if (!key.equals(OM_KEY_PREFIX)) {
            builder.append(key);
          }
        }
      }
      return builder.toString();
    }
  }
}
