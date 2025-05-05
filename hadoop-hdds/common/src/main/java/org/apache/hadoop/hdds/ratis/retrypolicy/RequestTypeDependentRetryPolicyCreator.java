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

package org.apache.hadoop.hdds.ratis.retrypolicy;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.ratis.client.retry.RequestTypeDependentRetryPolicy;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.exceptions.GroupMismatchException;
import org.apache.ratis.protocol.exceptions.NotReplicatedException;
import org.apache.ratis.protocol.exceptions.ResourceUnavailableException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.retry.ExceptionDependentRetry;
import org.apache.ratis.retry.ExponentialBackoffRetry;
import org.apache.ratis.retry.MultipleLinearRandomRetry;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.util.TimeDuration;

/**
 * Table mapping exception type to retry policy used for the exception in
 * write and watch request.
 * ---------------------------------------------------------------------------
 * |        Exception            | RetryPolicy for     | RetryPolicy for     |
 * |                             | Write request       | Watch request       |
 * |-------------------------------------------------------------------------|
 * | NotReplicatedException      | NO_RETRY            | NO_RETRY            |
 * |-------------------------------------------------------------------------|
 * | GroupMismatchException      | NO_RETRY            | NO_RETRY            |
 * |-------------------------------------------------------------------------|
 * | StateMachineException       | NO_RETRY            | NO_RETRY            |
 * |-------------------------------------------------------------------------|
 * | TimeoutIOException          | EXPONENTIAL_BACKOFF | NO_RETRY            |
 * |-------------------------------------------------------------------------|
 * | ResourceUnavailableException| EXPONENTIAL_BACKOFF | EXPONENTIAL_BACKOFF |
 * |-------------------------------------------------------------------------|
 * | Others                      | MULTILINEAR_RANDOM  | MULTILINEAR_RANDOM  |
 * |                             | _RETRY             | _RETRY               |
 * ---------------------------------------------------------------------------
 */
public class RequestTypeDependentRetryPolicyCreator
    implements RetryPolicyCreator {

  private static final Class[] NO_RETRY_EXCEPTIONS =
      new Class[] {NotReplicatedException.class, GroupMismatchException.class,
          StateMachineException.class};

  @Override
  public RetryPolicy create(ConfigurationSource conf) {
    RatisClientConfig ratisClientConfig = conf
        .getObject(RatisClientConfig.class);
    ExponentialBackoffRetry exponentialBackoffRetry =
        createExponentialBackoffPolicy(ratisClientConfig);
    MultipleLinearRandomRetry multipleLinearRandomRetry =
        MultipleLinearRandomRetry
            .parseCommaSeparated(ratisClientConfig.getMultilinearPolicy());

    return RequestTypeDependentRetryPolicy.newBuilder()
        .setRetryPolicy(RaftProtos.RaftClientRequestProto.TypeCase.WRITE,
            createExceptionDependentPolicy(exponentialBackoffRetry,
                multipleLinearRandomRetry, exponentialBackoffRetry))
        .setRetryPolicy(RaftProtos.RaftClientRequestProto.TypeCase.WATCH,
            createExceptionDependentPolicy(exponentialBackoffRetry,
                multipleLinearRandomRetry, RetryPolicies.noRetry()))
        .setTimeout(RaftProtos.RaftClientRequestProto.TypeCase.WRITE,
            toTimeDuration(ratisClientConfig.getWriteRequestTimeout()))
        .setTimeout(RaftProtos.RaftClientRequestProto.TypeCase.WATCH,
            toTimeDuration(ratisClientConfig.getWatchRequestTimeout()))
        .build();
  }

  private static ExponentialBackoffRetry createExponentialBackoffPolicy(
      RatisClientConfig ratisClientConfig) {
    return ExponentialBackoffRetry.newBuilder()
        .setBaseSleepTime(
            toTimeDuration(ratisClientConfig.getExponentialPolicyBaseSleep()))
        .setMaxSleepTime(
            toTimeDuration(ratisClientConfig.getExponentialPolicyMaxSleep()))
        .setMaxAttempts(
            ratisClientConfig.getExponentialPolicyMaxRetries())
        .build();
  }

  private static ExceptionDependentRetry createExceptionDependentPolicy(
      ExponentialBackoffRetry exponentialBackoffRetry,
      MultipleLinearRandomRetry multipleLinearRandomRetry,
      RetryPolicy timeoutPolicy) {
    ExceptionDependentRetry.Builder builder =
        ExceptionDependentRetry.newBuilder();
    for (Class c : NO_RETRY_EXCEPTIONS) {
      builder.setExceptionToPolicy(c, RetryPolicies.noRetry());
    }
    return builder.setExceptionToPolicy(ResourceUnavailableException.class,
        exponentialBackoffRetry)
        .setExceptionToPolicy(TimeoutIOException.class, timeoutPolicy)
        .setDefaultPolicy(multipleLinearRandomRetry)
        .build();
  }

  private static TimeDuration toTimeDuration(Duration duration) {
    return toTimeDuration(duration.toMillis());
  }

  private static TimeDuration toTimeDuration(long milliseconds) {
    return TimeDuration.valueOf(milliseconds, TimeUnit.MILLISECONDS);
  }
}
