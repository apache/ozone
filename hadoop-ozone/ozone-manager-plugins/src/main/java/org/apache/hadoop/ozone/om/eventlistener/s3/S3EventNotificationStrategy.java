package org.apache.hadoop.ozone.om.eventlistener.s3;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.eventlistener.OMEventListenerNotificationStrategy;
import org.apache.hadoop.ozone.om.helpers.OperationInfo;
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

  public List<String> determineEventsForOperation(OperationInfo operationInfo) {

    switch (operationInfo.getOpArgs().getOperationType()) {
      case CREATE_KEY:
      case COMMIT_KEY: {
        return Collections.singletonList(createS3Event("ObjectCreated:Put",
                operationInfo.getVolumeName(),
                operationInfo.getBucketName(),
                operationInfo.getKeyName()));
      }
      case DELETE_KEY: {
        return Collections.singletonList(createS3Event("ObjectRemoved:Delete",
                operationInfo.getVolumeName(),
                operationInfo.getBucketName(),
                operationInfo.getKeyName()));
      }
      case RENAME_KEY: {
        OperationInfo.OperationArgs.RenameKeyArgs renameKeyArgs
            = (OperationInfo.OperationArgs.RenameKeyArgs) operationInfo.getOpArgs();

        return Arrays.asList(
            createS3Event("ObjectCreated:Put",
                operationInfo.getVolumeName(),
                operationInfo.getBucketName(),
                renameKeyArgs.getToKeyName()),

            createS3Event("ObjectRemoved:Delete",
                operationInfo.getVolumeName(),
                operationInfo.getBucketName(),
                operationInfo.getKeyName())
        );
      }
      default:
        LOG.info("No events for operation {} on {}",
             operationInfo.getOpArgs().getOperationType(),
             operationInfo.getKeyName());
        return Collections.emptyList();
     }
   }

  static String createS3Event(String eventName, String volumeName, String bucketName, String keyName) {
    try {
      String objectKey = OzoneKeyUtil.getOzoneKey(volumeName, bucketName, keyName);
      String bucketArn = "arn:aws:s3:::" + volumeName + "." + bucketName;
      Instant eventTime = Instant.now();
      String etag = UUID.randomUUID().toString();

      S3EventNotification event = new S3EventNotificationBuilder(objectKey, bucketName, bucketArn, eventName, eventTime, etag)
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
