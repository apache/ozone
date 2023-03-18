/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3.throttler;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.s3.S3GatewayConfigKeys;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.Test;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.http.HttpServer2.HTTP_MAX_THREADS_KEY;
import static org.apache.hadoop.ozone.s3.signature.SignatureProcessor.DATE_FORMATTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Testing of DecayRequestScheduler.
 */
public class TestDecayRequestScheduler {
  private static final double DECAY_FACTOR = 0.5;
  private static final long DECAY_PERIOD = 2000;
  private static final long MAX_REQUESTS = 10;
  private static final double REJECTION_RATIO = 0.5;

  private static Request getRequest(String user) {
    final String curDate = DATE_FORMATTER.format(LocalDate.now());
    final String auth = "AWS4-HMAC-SHA256 " +
        "Credential=" + user + "/" + curDate + "/us-east-1/s3/aws4_request, " +
        "SignedHeaders=host;range;x-amz-date, " +
        "Signature=fe5f80f77d5fa3beca038a248ff027";

    return new Request() {
      @Override
      public Map<String, String> getHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", auth);
        return headers;
      }

      @Override
      public Map<String, String> getQueryParameters() {
        return new HashMap<>();
      }
    };
  }

  private RequestScheduler getRequestScheduler() {
    OzoneConfiguration config = new OzoneConfiguration();
    config.setLong(HTTP_MAX_THREADS_KEY, 1);
    config.setDouble(S3GatewayConfigKeys.OZONE_S3G_DECAYSCHEDULER_FACTOR_KEY,
        DECAY_FACTOR);
    config.setLong(S3GatewayConfigKeys.OZONE_S3G_DECAYSCHEDULER_PERIOD_KEY,
        DECAY_PERIOD);
    config.setDouble(
        S3GatewayConfigKeys.OZONE_S3G_DECAYSCHEDULER_REJECTION_RATIO_KEY,
        REJECTION_RATIO);
    return new DecayRequestScheduler(new UserIdentityProvider(), config);
  }


  @Test
  public void testSingleUserNoPriorRequests() throws Exception {
    RequestScheduler requestScheduler = getRequestScheduler();
    String user = "ozone";
    Request request = getRequest(user);

    assertFalse("Expected user " + user + " request to be granted",
        requestScheduler.shouldReject(request));
  }

  @Test
  public void testSingleUserFewRequests() throws Exception {
    RequestScheduler requestScheduler = getRequestScheduler();
    String user = "ozone";
    Request request = getRequest(user);
    requestScheduler.addRequest(request);

    assertFalse("Expected user " + user + " request to be granted",
        requestScheduler.shouldReject(request));
  }

  @Test(timeout = 6000)
  public void testSingleUserAfterDecay() throws Exception {
    RequestScheduler requestScheduler = getRequestScheduler();
    Request testRequest = getRequest("testUser");

    // total requests = 6 after this.
    for (int i = 0; i < 4; i++) {
      requestScheduler.addRequest(testRequest);
    }

    // Wait for a decay.
    TimeUnit.MILLISECONDS.sleep(DECAY_PERIOD);

    String user = "ozone";
    Request request = getRequest(user);

    for (int i = 0; i < 3; i++) {
      requestScheduler.addRequest(request);
    }

    // Total requests = 6, ozone made 4 requests (should be rejected now).
    assertTrue("Expected user " + user + " request to be rejected",
        requestScheduler.shouldReject(request));
  }

  @Test(timeout = 6000)
  public void testMultipleUsersWithDecay() throws Exception {
    // Prepare users and requests for testing
    String[] users = {"user1", "user2", "user3"};
    long[] requestsCount =
        {MAX_REQUESTS - 2, MAX_REQUESTS + 2, MAX_REQUESTS * 2};
    boolean[] firstDecay = {false, false, true};
    boolean[] secondDecay = {false, false, false};
    final int usersCount = users.length;
    Request[] requests = new Request[usersCount];
    for (int i = 0; i < usersCount; i++) {
      requests[i] = getRequest(users[i]);
    }

    // maxRequests = 0 at the beginning.
    RequestScheduler requestScheduler = getRequestScheduler();

    // Add different requests count for each user.
    for (int i = 0; i < usersCount; i++) {
      for (int j = 0; j < requestsCount[i]; j++) {
        requestScheduler.addRequest(requests[i]);
      }
    }

    // Wait for a decay.
    // After a decay: 4, 6, 10 (maxRequests = 20)
    TimeUnit.MILLISECONDS.sleep(DECAY_PERIOD);

    // requests for each user: 4+8, 6+12, 10+20 (maxRequests = 20)
    // user3 should be rejected.
    for (int i = 0; i < usersCount; i++) {
      for (int j = 0; j < requestsCount[i]; j++) {
        requestScheduler.addRequest(requests[i]);
      }
    }

    for (int i = 0; i < usersCount; i++) {
      assertEquals(
          "Expected user " + users[i] + " request to be " +
              (firstDecay[i] ? "rejected" : "granted"),
          firstDecay[i], requestScheduler.shouldReject(requests[i]));
    }

    // Wait for a decay.
    // After a decay: 6, 9, 15 (maxRequests = 20)
    LambdaTestUtils.await((int) DECAY_PERIOD, 500, () ->
        !requestScheduler.shouldReject(requests[2]));

    // All users should pass now.
    for (int i = 0; i < usersCount; i++) {
      assertEquals(
          "Expected user " + users[i] + " request to be " +
              (secondDecay[i] ? "rejected" : "granted"),
          secondDecay[i], requestScheduler.shouldReject(requests[i]));
    }
  }
}
