/*
 * Copyright 2014-2025 Amazon Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    http://aws.amazon.com/apache2.0
 *
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
 * OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.eventlistener.s3;

/* copy of
 * software.amazon.awssdk.eventnotifications.s3.model.S3EventNotification
 * class taken from AWS SDK (1.x) with minor changes for build issues
 * and removed usage of unnecessary AWS specific extension entiies:
 *
 * - GlacierEventDataEntity
 * - LifecycleEventDataEntity
 * - IntelligentTieringEventDataEntity
 * - ReplicationEventDataEntity
 *
 * NOTE: We may not need to fork this class if we can use the SDK one directly
 * but conversely we may want to make our own customizations.
 */

import java.util.List;
import org.joda.time.DateTime;

//import com.amazonaws.internal.DateTimeJsonSerializer;
//import com.amazonaws.util.json.Jackson;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;

/**
* A helper class that represents a strongly typed S3 EventNotification item sent
* to SQS, SNS, or Lambda.
 *
 * <p>
 * <b>Migrating to the AWS SDK for Java v2</b>
 * <p>
 * The v2 equivalent of this class is
 * <a href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/eventnotifications/s3/model/S3EventNotification.html">S3EventNotification</a>
 *
 * <p>
 * See <a href="https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/migration.html">Migration Guide</a>
 * for more information.
*/
public class S3EventNotification {

    private final List<S3EventNotificationRecord> records;

    @JsonCreator
    public S3EventNotification(
            @JsonProperty(value = "Records") List<S3EventNotificationRecord> records)
    {
        this.records = records;
    }

    /**
     * <p>
     * Parse the JSON string into a S3EventNotification object.
     * </p>
     * <p>
     * The function will try its best to parse input JSON string as best as it can.
     * It will not fail even if the JSON string contains unknown properties.
     * The function will throw SdkClientException if the input JSON string is
     * not valid JSON.
     * </p>
     * @param json
     *         JSON string to parse. Typically this is the body of your SQS
     *         notification message body.
     *
     * @return The resulting S3EventNotification object.
     */
    //public static S3EventNotification parseJson(String json) {
    //    return Jackson.fromJsonString(json, S3EventNotification.class);
    //}

    /**
     * @return the records in this notification
     */
    @JsonProperty(value = "Records")
    public List<S3EventNotificationRecord> getRecords() {
        return records;
    }

    /**
     * @return a JSON representation of this object
     */
    //public String toJson() {
    //    return Jackson.toJsonString(this);
    //}

    public static class UserIdentityEntity {

        private final String principalId;

        @JsonCreator
        public UserIdentityEntity(
                @JsonProperty(value = "principalId") String principalId) {
            this.principalId = principalId;
        }

        public String getPrincipalId() {
            return principalId;
        }
    }

    public static class S3BucketEntity {

        private final String name;
        private final UserIdentityEntity ownerIdentity;
        private final String arn;

        @JsonCreator
        public S3BucketEntity(
                @JsonProperty(value = "name") String name,
                @JsonProperty(value = "ownerIdentity") UserIdentityEntity ownerIdentity,
                @JsonProperty(value = "arn") String arn)
        {
            this.name = name;
            this.ownerIdentity = ownerIdentity;
            this.arn = arn;
        }

        public String getName() {
            return name;
        }

        public UserIdentityEntity getOwnerIdentity() {
            return ownerIdentity;
        }

        public String getArn() {
            return arn;
        }
    }

    public static class S3ObjectEntity {

        private final String key;
        private final Long size;
        private final String eTag;
        private final String versionId;
        private final String sequencer;

        @Deprecated
        public S3ObjectEntity(
                String key,
                Integer size,
                String eTag,
                String versionId)
        {
            this.key = key;
            this.size = size == null ? null : size.longValue();
            this.eTag = eTag;
            this.versionId = versionId;
            this.sequencer = null;
        }

        @Deprecated
        public S3ObjectEntity(
                String key,
                Long size,
                String eTag,
                String versionId)
        {
            this(key, size, eTag, versionId, null);
        }

        @JsonCreator
        public S3ObjectEntity(
                @JsonProperty(value = "key") String key,
                @JsonProperty(value = "size") Long size,
                @JsonProperty(value = "eTag") String eTag,
                @JsonProperty(value = "versionId") String versionId,
                @JsonProperty(value = "sequencer") String sequencer)
        {
            this.key = key;
            this.size = size;
            this.eTag = eTag;
            this.versionId = versionId;
            this.sequencer = sequencer;
        }

        public String getKey() {
            return key;
        }

        /**
         * S3 URL encodes the key of the object involved in the event. This is
         * a convenience method to automatically URL decode the key.
         * @return The URL decoded object key.
         */
        //public String getUrlDecodedKey() {
        //    return SdkHttpUtils.urlDecode(getKey());
        //}

        /**
         * @deprecated use {@link #getSizeAsLong()} instead.
         */
        @Deprecated
        @JsonIgnore
        public Integer getSize() {
            return size == null ? null : size.intValue();
        }

        @JsonProperty(value = "size")
        public Long getSizeAsLong() {
            return size;
        }

        public String geteTag() {
            return eTag;
        }

        public String getVersionId() {
            return versionId;
        }

        public String getSequencer() {
            return sequencer;
        }
    }

    public static class S3Entity {

        private final String configurationId;
        private final S3BucketEntity bucket;
        private final S3ObjectEntity object;
        private final String s3SchemaVersion;

        @JsonCreator
        public S3Entity(
                @JsonProperty(value = "configurationId") String configurationId,
                @JsonProperty(value = "bucket") S3BucketEntity bucket,
                @JsonProperty(value = "object") S3ObjectEntity object,
                @JsonProperty(value = "s3SchemaVersion") String s3SchemaVersion)
        {
            this.configurationId = configurationId;
            this.bucket = bucket;
            this.object = object;
            this.s3SchemaVersion = s3SchemaVersion;
        }

        public String getConfigurationId() {
            return configurationId;
        }

        public S3BucketEntity getBucket() {
            return bucket;
        }

        public S3ObjectEntity getObject() {
            return object;
        }

        public String getS3SchemaVersion() {
            return s3SchemaVersion;
        }
    }

    public static class RequestParametersEntity {

        private final String sourceIPAddress;

        @JsonCreator
        public RequestParametersEntity(
                @JsonProperty(value = "sourceIPAddress") String sourceIPAddress)
        {
            this.sourceIPAddress = sourceIPAddress;
        }

        public String getSourceIPAddress() {
            return sourceIPAddress;
        }
    }

    public static class ResponseElementsEntity {

        private final String xAmzId2;
        private final String xAmzRequestId;

        @JsonCreator
        public ResponseElementsEntity(
                @JsonProperty(value = "x-amz-id-2") String xAmzId2,
                @JsonProperty(value = "x-amz-request-id") String xAmzRequestId)
        {
            this.xAmzId2 = xAmzId2;
            this.xAmzRequestId = xAmzRequestId;
        }

        @JsonProperty("x-amz-id-2")
        public String getxAmzId2() {
            return xAmzId2;
        }

        @JsonProperty("x-amz-request-id")
        public String getxAmzRequestId() {
            return xAmzRequestId;
        }
    }

    public static class GlacierEventDataEntity {
        private final RestoreEventDataEntity restoreEventData;

        @JsonCreator
        public GlacierEventDataEntity(
                @JsonProperty(value = "restoreEventData") RestoreEventDataEntity restoreEventData)
        {
            this.restoreEventData = restoreEventData;
        }

        public RestoreEventDataEntity getRestoreEventData() {
            return restoreEventData;
        }
    }

    public static class LifecycleEventDataEntity {

        private final TransitionEventDataEntity transitionEventData;

        @JsonCreator
        public LifecycleEventDataEntity(
                @JsonProperty(value = "transitionEventData") TransitionEventDataEntity transitionEventData)
        {

            this.transitionEventData = transitionEventData;
        }

        public TransitionEventDataEntity getTransitionEventData() {
            return transitionEventData;
        }
    }

    public static class IntelligentTieringEventDataEntity {

        private final String destinationAccessTier;

        @JsonCreator
        public IntelligentTieringEventDataEntity(
                @JsonProperty(value = "destinationAccessTier") String destinationAccessTier)
        {
            this.destinationAccessTier = destinationAccessTier;
        }

        @JsonProperty("destinationAccessTier")
        public String getDestinationAccessTier() {
            return destinationAccessTier;
        }
    }

    public static class ReplicationEventDataEntity {

        private final String replicationRuleId;
        private final String destinationBucket;
        private final String s3Operation;
        private final String requestTime;
        private final String failureReason;
        private final String threshold;
        private final String replicationTime;

        @JsonCreator
        public ReplicationEventDataEntity(
                @JsonProperty(value = "replicationRuleId") String replicationRuleId,
                @JsonProperty(value = "destinationBucket") String destinationBucket,
                @JsonProperty(value = "s3Operation") String s3Operation,
                @JsonProperty(value = "requestTime") String requestTime,
                @JsonProperty(value = "failureReason") String failureReason,
                @JsonProperty(value = "threshold") String threshold,
                @JsonProperty(value = "replicationTime") String replicationTime)
        {
            this.replicationRuleId = replicationRuleId;
            this.destinationBucket = destinationBucket;
            this.s3Operation = s3Operation;
            this.requestTime = requestTime;
            this.failureReason = failureReason;
            this.threshold = threshold;
            this.replicationTime = replicationTime;
        }

        @JsonProperty("replicationRuleId")
        public String getReplicationRuleId() {
            return replicationRuleId;
        }
        @JsonProperty("destinationBucket")
        public String getDestinationBucket() {
            return destinationBucket;
        }
        @JsonProperty("s3Operation")
        public String getS3Operation() {
            return s3Operation;
        }
        @JsonProperty("requestTime")
        public String getRequestTime() {
            return requestTime;
        }
        @JsonProperty("failureReason")
        public String getFailureReason() {
            return failureReason;
        }
        @JsonProperty("threshold")
        public String getThreshold() {
            return threshold;
        }
        @JsonProperty("replicationTime")
        public String getReplicationTime() {
            return replicationTime;
        }
    }

    public static class TransitionEventDataEntity {
        private final String destinationStorageClass;

        @JsonCreator
        public TransitionEventDataEntity(
                @JsonProperty("destinationStorageClass") String destinationStorageClass)
        {
            this.destinationStorageClass = destinationStorageClass;
        }

        public String getDestinationStorageClass() {
            return destinationStorageClass;
        }
    }

    public static class RestoreEventDataEntity {
        private DateTime lifecycleRestorationExpiryTime;
        private final String lifecycleRestoreStorageClass;

        @JsonCreator
        public RestoreEventDataEntity(
                @JsonProperty("lifecycleRestorationExpiryTime") String lifecycleRestorationExpiryTime,
                @JsonProperty("lifecycleRestoreStorageClass") String lifecycleRestoreStorageClass)
        {
            if (lifecycleRestorationExpiryTime != null) {
                this.lifecycleRestorationExpiryTime = DateTime.parse(lifecycleRestorationExpiryTime);
            }
            this.lifecycleRestoreStorageClass = lifecycleRestoreStorageClass;
        }

        @JsonSerialize(using=DateTimeJsonSerializer.class)
        public DateTime getLifecycleRestorationExpiryTime() {
            return lifecycleRestorationExpiryTime;
        }

        public String getLifecycleRestoreStorageClass() {
            return lifecycleRestoreStorageClass;
        }
    }

    public static class S3EventNotificationRecord {

        private final String awsRegion;
        private final String eventName;
        private final String eventSource;
        private DateTime eventTime;
        private final String eventVersion;
        private final RequestParametersEntity requestParameters;
        private final ResponseElementsEntity responseElements;
        private final S3Entity s3;
        private final UserIdentityEntity userIdentity;
        private final Map<String, String> eventData;
        //private final GlacierEventDataEntity glacierEventData;
        //private final LifecycleEventDataEntity lifecycleEventData;
        //private final IntelligentTieringEventDataEntity intelligentTieringEventData;
        //private final ReplicationEventDataEntity replicationEventDataEntity;

        /*
        @Deprecated
        public S3EventNotificationRecord(
                String awsRegion,
                String eventName,
                String eventSource,
                String eventTime,
                String eventVersion,
                RequestParametersEntity requestParameters,
                ResponseElementsEntity responseElements,
                S3Entity s3,
                UserIdentityEntity userIdentity)
        {
            this(awsRegion,
                 eventName,
                 eventSource,
                 eventTime,
                 eventVersion,
                 requestParameters,
                 responseElements,
                 s3,
                 userIdentity,
                 null,
                 null,
                 null,
                 null);
        }

        @Deprecated
        public S3EventNotificationRecord(
                String awsRegion,
                String eventName,
                String eventSource,
                String eventTime,
                String eventVersion,
                RequestParametersEntity requestParameters,
                ResponseElementsEntity responseElements,
                S3Entity s3,
                UserIdentityEntity userIdentity,
                GlacierEventDataEntity glacierEventData)
        {
            this(awsRegion,
                    eventName,
                    eventSource,
                    eventTime,
                    eventVersion,
                    requestParameters,
                    responseElements,
                    s3,
                    userIdentity,
                    glacierEventData,
                    null,
                    null,
                    null);
        }
        */

        @JsonCreator
        public S3EventNotificationRecord(
                @JsonProperty(value = "awsRegion") String awsRegion,
                @JsonProperty(value = "eventName") String eventName,
                @JsonProperty(value = "eventSource") String eventSource,
                @JsonProperty(value = "eventTime") String eventTime,
                @JsonProperty(value = "eventVersion") String eventVersion,
                @JsonProperty(value = "requestParameters") RequestParametersEntity requestParameters,
                @JsonProperty(value = "responseElements") ResponseElementsEntity responseElements,
                @JsonProperty(value = "s3") S3Entity s3,
                @JsonProperty(value = "userIdentity") UserIdentityEntity userIdentity,
                @JsonProperty(value = "eventData") Map<String, String> eventData)
                //@JsonProperty(value = "glacierEventData") GlacierEventDataEntity glacierEventData,
                //@JsonProperty(value = "lifecycleEventData") LifecycleEventDataEntity lifecycleEventData,
                //@JsonProperty(value = "intelligentTieringEventData") IntelligentTieringEventDataEntity intelligentTieringEventData,
                //@JsonProperty(value = "replicationEventData") ReplicationEventDataEntity replicationEventData)
        {
            this.awsRegion = awsRegion;
            this.eventName = eventName;
            this.eventSource = eventSource;

            if (eventTime != null)
            {
                this.eventTime = DateTime.parse(eventTime);
            }

            this.eventVersion = eventVersion;
            this.requestParameters = requestParameters;
            this.responseElements = responseElements;
            this.s3 = s3;
            this.userIdentity = userIdentity;
            this.eventData = eventData;
            //this.glacierEventData = glacierEventData;
            //this.lifecycleEventData = lifecycleEventData;
            //this.intelligentTieringEventData = intelligentTieringEventData;
            //this.replicationEventDataEntity = replicationEventData;
        }

        public String getAwsRegion() {
            return awsRegion;
        }

        public String getEventName() {
            return eventName;
        }

        //@JsonIgnore
        //public S3Event getEventNameAsEnum() {
        //    return S3Event.fromValue(eventName);
        //}

        public String getEventSource() {
            return eventSource;
        }

        @JsonSerialize(using=DateTimeJsonSerializer.class)
        public DateTime getEventTime() {
            return eventTime;
        }

        public String getEventVersion() {
            return eventVersion;
        }

        public RequestParametersEntity getRequestParameters() {
            return requestParameters;
        }

        public ResponseElementsEntity getResponseElements() {
            return responseElements;
        }

        public S3Entity getS3() {
            return s3;
        }

        public UserIdentityEntity getUserIdentity() {
            return userIdentity;
        }

        // Ozone extension
        public Map<String, String> getEventData() {
            return eventData;
        }

        //public GlacierEventDataEntity getGlacierEventData() {
        //    return glacierEventData;
        //}

        //public LifecycleEventDataEntity getLifecycleEventData() { return lifecycleEventData; }

        //public IntelligentTieringEventDataEntity getIntelligentTieringEventData() { return intelligentTieringEventData; }

        //public ReplicationEventDataEntity getReplicationEventDataEntity() { return replicationEventDataEntity; }

    }
}
