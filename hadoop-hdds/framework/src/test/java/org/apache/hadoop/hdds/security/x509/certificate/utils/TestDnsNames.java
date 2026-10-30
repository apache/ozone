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

package org.apache.hadoop.hdds.security.x509.certificate.utils;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DnsNames}.
 */
public class TestDnsNames {

  @Test
  public void acceptsValidDnsNames() {
    String[] valid = {
        "scm1.lxd",
        "datanode1",
        "om.internal",
        "dn3.local",
        "a-b.c-d.example.com",
        "hadoop.apache.org",
        StringUtils.repeat('a', 63), // 63-character label
    };
    for (String name : valid) {
      assertTrue(DnsNames.isValidDnsName(name), name);
      assertTrue(DnsNames.toDnsSanValue(name).isPresent(), name);
    }
  }

  @Test
  public void rejectsInvalidDnsNames() {
    String longLabel = StringUtils.repeat('a', 64); // 64-character label
    String longName = StringUtils.repeat("a234567890.", 24) + "example.com"; // 275 chars, > 253
    String[] invalid = {
        "",
        " ",
        "*.example.com",
        "10.0.0.5",
        "2001:db8::1",
        "-lead.example.com",
        "trail-.example.com",
        "host_name.lxd",
        longLabel,
        longName,
    };
    for (String name : invalid) {
      assertFalse(DnsNames.isValidDnsName(name), name);
      assertFalse(DnsNames.toDnsSanValue(name).isPresent(), name);
    }
  }

  @Test
  public void doesNotThrowForNull() {
    assertDoesNotThrow(() -> DnsNames.isValidDnsName(null));
    assertDoesNotThrow(() -> DnsNames.toDnsSanValue(null));
    assertFalse(DnsNames.isValidDnsName(null));
    assertFalse(DnsNames.toDnsSanValue(null).isPresent());
  }

  @Test
  public void stripsAtMostOneTrailingDot() {
    assertEquals(Optional.of("scm1.lxd"), DnsNames.toDnsSanValue("scm1.lxd."));
    assertFalse(DnsNames.isValidDnsName("scm1.lxd."));
    assertFalse(DnsNames.toDnsSanValue("scm1.lxd..").isPresent());
  }

  @Test
  public void handlesIdnConversion() {
    assertEquals(Optional.of("xn--bcher-kva.lxd"), DnsNames.toDnsSanValue("bücher.lxd"));
    assertFalse(DnsNames.isValidDnsName("bücher.lxd"));
  }
}
