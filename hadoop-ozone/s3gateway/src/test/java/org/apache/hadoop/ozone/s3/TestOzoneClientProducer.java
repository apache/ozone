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
package org.apache.hadoop.ozone.s3;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.OMConfigKeys;

import static org.junit.Assert.fail;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test class for @{@link OzoneClientProducer}.
 */
@RunWith(Parameterized.class)
public class TestOzoneClientProducer {

  private OzoneClientProducer producer;

  public TestOzoneClientProducer(
      String authHeader, String contentMd5,
      String host, String amzContentSha256, String date, String contentType
  )
      throws Exception {
    producer = new OzoneClientProducer();
    OzoneConfiguration config = new OzoneConfiguration();
    config.setBoolean(OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY, true);
    config.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, "");
    producer.setOzoneConfiguration(config);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {
            "AWS4-HMAC-SHA256 Credential=testuser1/20190221/us-west-1/s3" +
                "/aws4_request, SignedHeaders=content-md5;host;" +
                "x-amz-content-sha256;x-amz-date, " +
                "Signature" +
                "=56ec73ba1974f8feda8365c3caef89c5d4a688d5f9baccf47" +
                "65f46a14cd745ad",
            "Zi68x2nPDDXv5qfDC+ZWTg==",
            "s3g:9878",
            "e2bd43f11c97cde3465e0e8d1aad77af7ec7aa2ed8e213cd0e24" +
                "1e28375860c6",
            "20190221T002037Z",
            ""
        },
        {
            "AWS4-HMAC-SHA256 " +
                "Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request," +
                " SignedHeaders=content-type;host;x-amz-date, " +
                "Signature=" +
                "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400" +
                "e06b5924a6f2b5d7",
            "",
            "iam.amazonaws.com",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "20150830T123600Z",
            "application/x-www-form-urlencoded; charset=utf-8"
        },
        {
            null, null, null, null, null, null
        }
    });
  }

  @Test
  public void testGetClientFailure() {
    try {
      producer.createClient();
      fail("testGetClientFailure");
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof IOException);
    }
  }

}
