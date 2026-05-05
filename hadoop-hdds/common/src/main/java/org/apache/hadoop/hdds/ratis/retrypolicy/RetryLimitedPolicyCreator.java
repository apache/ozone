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

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.util.TimeDuration;

/**
 * The creator of RetryLimited policy.
 */
public class RetryLimitedPolicyCreator implements RetryPolicyCreator {

  @Override
  public RetryPolicy create(ConfigurationSource conf) {
    RatisClientConfig scmClientConfig =
        conf.getObject(RatisClientConfig.class);
    int maxRetryCount =
        scmClientConfig.getRetrylimitedMaxRetries();
    long retryInterval = scmClientConfig.getRetrylimitedRetryInterval();
    TimeDuration sleepDuration =
        TimeDuration.valueOf(retryInterval, TimeUnit.MILLISECONDS);
    RetryPolicy retryPolicy = RetryPolicies
        .retryUpToMaximumCountWithFixedSleep(maxRetryCount, sleepDuration);
    return retryPolicy;
  }
}
