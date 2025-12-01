package org.apache.hadoop.ozone.om.eventlistener.s3;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.ozone.om.eventlistener.s3.S3EventNotification.S3BucketEntity;
import org.apache.hadoop.ozone.om.eventlistener.s3.S3EventNotification.S3EventNotificationRecord;
import org.apache.hadoop.ozone.om.eventlistener.s3.S3EventNotification.S3ObjectEntity;
import org.apache.hadoop.ozone.om.eventlistener.s3.S3EventNotification.UserIdentityEntity;

/**
 * This is a builder for the AWS event notification class
 * com.amazonaws.services.s3.event.S3EventNotification which is part of
 * AWS SDK 1.x
 *
 * NOTE: the above class is designed for deserialization which is why it
 * requires this builder wrapper.
 *
 * XXX: we may need to fork these classes so that we can customize it to
 * our needs.
 */
public class S3EventNotificationBuilder {

  private static final String REGION = "us-east-1";
  private static final String BUCKET_ARN_PREFIX = "arn:aws:s3:" + REGION;
  private static final String EVENT_SOURCE = "ozone:s3";
  private static final String EVENT_VERSION = "2.1";
  private static final String USER_IDENTITY = "some-principalId";

  private static final String SCHEMA_VERSION = "1.0";
  private static final String CONFIGURATION_ID = "mynotif1";

  private final String objectKey;
  private final String bucketName;
  private final String bucketArn;
  private final String eventName;
  private final Instant eventTime;
  private final String etag;
  private final Map<String, String> eventData;

  // mutable fields defaulting to null
  private Long objectSize;
  private String objectVersionId;
  private String objectSequencer;

  public S3EventNotificationBuilder(String objectKey, String bucketName, String bucketArn, String eventName, Instant eventTime, String etag) {
    this.objectKey = objectKey;
    this.bucketName = bucketName;
    this.bucketArn = bucketArn;
    this.eventName = eventName;
    this.eventTime = eventTime;
    this.etag = etag;
    this.eventData = new HashMap<>();
  }

  public S3EventNotificationBuilder setObjectSize(long objectSize) {
    this.objectSize = objectSize;
    return this;
  }

  public S3EventNotificationBuilder setObjectVersionId(String objectVersionId) {
    this.objectVersionId = objectVersionId;
    return this;
  }

  public S3EventNotificationBuilder setObjectSequencer(String objectSequencer) {
    this.objectSequencer = objectSequencer;
    return this;
  }

  public S3EventNotificationBuilder addAllEventData(Map<String, String> eventData) {
    this.eventData.putAll(eventData);
    return this;
  }

  public S3EventNotification build() {
    UserIdentityEntity userIdentity = new UserIdentityEntity(USER_IDENTITY);
    S3BucketEntity s3BucketEntity = new S3BucketEntity(bucketName, userIdentity, bucketArn);

    S3EventNotification.S3ObjectEntity s3ObjectEntity = new S3ObjectEntity(
        objectKey,
        objectSize,
        etag,
        objectVersionId,
        objectSequencer);

    S3EventNotification.S3Entity s3Entity = new S3EventNotification.S3Entity(
        CONFIGURATION_ID,
        s3BucketEntity,
        s3ObjectEntity,
        SCHEMA_VERSION);

   S3EventNotificationRecord eventRecord = new S3EventNotificationRecord(
       REGION,
       eventName,
       EVENT_SOURCE,
       eventTime.toString(),
       EVENT_VERSION,
       new S3EventNotification.RequestParametersEntity(""),
       new S3EventNotification.ResponseElementsEntity("", ""),
       s3Entity,
       new S3EventNotification.UserIdentityEntity("tester"),
       eventData);

    return new S3EventNotification(Collections.singletonList(eventRecord));
  }
}
