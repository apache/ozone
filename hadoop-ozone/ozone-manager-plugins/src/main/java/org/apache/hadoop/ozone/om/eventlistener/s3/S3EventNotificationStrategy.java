package org.apache.hadoop.ozone.om.eventlistener.s3;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.eventlistener.OMEventListenerNotificationStrategy;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * This is a notification strategy to generate events according to S3
 * notification semantics
 */
public class S3EventNotificationStrategy implements OMEventListenerNotificationStrategy {
  public static final Logger LOG = LoggerFactory.getLogger(S3EventNotificationStrategy.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public List<String> determineEventsForOperation(OmCompletedRequestInfo requestInfo) {

    switch (requestInfo.getOpArgs().getOperationType()) {
      case CREATE_KEY:
      case COMMIT_KEY: {
        return Collections.singletonList(createS3Event("ObjectCreated:Put",
                requestInfo.getVolumeName(),
                requestInfo.getBucketName(),
                requestInfo.getKeyName(),
                Collections.emptyMap())
        );
      }
      case CREATE_FILE: {
        OmCompletedRequestInfo.OperationArgs.CreateFileArgs createFileArgs
            = (OmCompletedRequestInfo.OperationArgs.CreateFileArgs) requestInfo.getOpArgs();

        // XXX: eventDaata is an Ozone extension. Its is unclear if this
        // schema makes sense but the general S3 schema is somewhat
        // freeform.  These arguments are more informational than
        // required so it is unclear as to their necessity.
        Map<String, String> eventData = new HashMap<>();
        eventData.put("isDirectory", "false");
        eventData.put("isRecursive", String.valueOf(createFileArgs.isRecursive()));
        eventData.put("isOverwrite", String.valueOf(createFileArgs.isOverwrite()));

        return Collections.singletonList(createS3Event("ObjectCreated:Put",
                requestInfo.getVolumeName(),
                requestInfo.getBucketName(),
                requestInfo.getKeyName(),
                eventData)
        );
      }
      case CREATE_DIRECTORY: {
        OmCompletedRequestInfo.OperationArgs.CreateDirectoryArgs createDirArgs
            = (OmCompletedRequestInfo.OperationArgs.CreateDirectoryArgs) requestInfo.getOpArgs();

        // XXX: eventDaata is an Ozone extension. Its is unclear if this
        // schema makes sense but the general S3 schema is somewhat
        // freeform.  These arguments are more informational than
        // required so it is unclear as to their necessity.
        Map<String, String> eventData = new HashMap<>();
        eventData.put("isDirectory", "true");

        return Collections.singletonList(createS3Event("ObjectCreated:Put",
                requestInfo.getVolumeName(),
                requestInfo.getBucketName(),
                requestInfo.getKeyName(),
                eventData)
        );
      }
      case DELETE_KEY: {
        return Collections.singletonList(createS3Event("ObjectRemoved:Delete",
                requestInfo.getVolumeName(),
                requestInfo.getBucketName(),
                requestInfo.getKeyName(),
                Collections.emptyMap())
        );
      }
      case RENAME_KEY: {
        OmCompletedRequestInfo.OperationArgs.RenameKeyArgs renameKeyArgs
            = (OmCompletedRequestInfo.OperationArgs.RenameKeyArgs) requestInfo.getOpArgs();

        String renameFromKey = OzoneKeyUtil.getOzoneKey(requestInfo.getVolumeName(),
                                                        requestInfo.getBucketName(),
                                                        requestInfo.getKeyName());

        // XXX: it would be good to be able to convey that this was a
        // file vs directory rename
        Map<String, String> eventData = new HashMap<>();
        eventData.put("renameFromKey", renameFromKey);

        // NOTE: ObjectRenamed:Rename is an Ozone extension as is the
        // eventData map in the S3 event schema.
        return Collections.singletonList(createS3Event("ObjectRenamed:Rename",
                requestInfo.getVolumeName(),
                requestInfo.getBucketName(),
                renameKeyArgs.getToKeyName(),
                eventData)
        );
      }
      default:
        LOG.info("No events for operation {} on {}",
             requestInfo.getOpArgs().getOperationType(),
             requestInfo.getKeyName());
        return Collections.emptyList();
     }
   }

  static String createS3Event(String eventName,
                              String volumeName,
                              String bucketName,
                              String keyName,
                              Map<String, String> eventData) {
    try {
      String objectKey = OzoneKeyUtil.getOzoneKey(volumeName, bucketName, keyName);
      String bucketArn = "arn:aws:s3:::" + volumeName + "." + bucketName;
      Instant eventTime = Instant.now();
      String etag = UUID.randomUUID().toString();

      S3EventNotification event = new S3EventNotificationBuilder(objectKey, bucketName, bucketArn, eventName, eventTime, etag)
          .addAllEventData(eventData)
          .build();

      return MAPPER.writer().writeValueAsString(event);
    } catch (Exception ex) {
      LOG.info("------------> {}", "failed");
      return null;
    }
  }

  // stub: taken from metadataManager.  Should we just pass in
  // metadataManager?
  private static class OzoneKeyUtil {
    public static String getOzoneKey(String volume, String bucket, String key) {
      StringBuilder builder = new StringBuilder()
         .append(volume);
      // TODO : Throw if the Bucket is null?
      builder.append(OM_KEY_PREFIX).append(bucket);
      if (StringUtils.isNotBlank(key)) {
        builder.append(OM_KEY_PREFIX);
        if (!key.equals(OM_KEY_PREFIX)) {
          builder.append(key);
        }
      }
      return builder.toString();
    }
  }
}
