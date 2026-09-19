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

package org.apache.hadoop.ozone.s3.endpoint;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import javax.ws.rs.WebApplicationException;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.junit.jupiter.api.Test;

/**
 * Tests XML parsing for {@link PutBucketLifecycleConfigurationUnmarshaller}.
 */
public class TestPutBucketLifecycleConfigurationUnmarshaller {

  @Test
  public void fromStreamWithNamespace() {
    ByteArrayInputStream inputBody = new ByteArrayInputStream(
        ("<LifecycleConfiguration xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">" +
            "<Rule>" +
            "<ID>expire after 1 day</ID>" +
            "<Status>Enabled</Status>" +
            "<Expiration><Days>1</Days></Expiration>" +
            "</Rule>" +
            "</LifecycleConfiguration>")
            .getBytes(UTF_8));

    S3LifecycleConfiguration configuration =
        new PutBucketLifecycleConfigurationUnmarshaller().readFrom(inputBody);

    assertNotNull(configuration);
    assertEquals(1, configuration.getRules().size());
    assertEquals("expire after 1 day", configuration.getRules().get(0).getId());
  }

  @Test
  public void lifecycleXmlWithDoctypeIsRejected() {
    String xml = "<?xml version=\"1.0\"?>\n"
        + "<!DOCTYPE LifecycleConfiguration ["
        + "<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]>\n"
        + "<LifecycleConfiguration xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">"
        + "<Rule>"
        + "<ID>rule1</ID>"
        + "<Status>Enabled</Status>"
        + "<Expiration><Days>1</Days></Expiration>"
        + "</Rule>"
        + "</LifecycleConfiguration>";

    WebApplicationException ex = assertThrows(WebApplicationException.class,
        () -> new PutBucketLifecycleConfigurationUnmarshaller()
            .readFrom(new ByteArrayInputStream(xml.getBytes(UTF_8))));

    assertTrue(containsDisallowDoctypeDecl(ex),
        "Expected parser to reject DOCTYPE declarations");
  }

  private static boolean containsDisallowDoctypeDecl(Throwable throwable) {
    for (Throwable current = throwable; current != null; current = current.getCause()) {
      if (current.getMessage() != null
          && current.getMessage().contains("disallow-doctype-decl")) {
        return true;
      }
    }
    return false;
  }
}
