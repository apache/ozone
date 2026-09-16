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

package org.apache.hadoop.ozone.recon.chatbot.recon;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link ReconApiAllowlist}. */
public class TestReconApiAllowlist {

  private ReconApiAllowlist catalog;

  @BeforeEach
  public void setUp() {
    catalog = new ReconApiAllowlist();
  }

  @Test
  public void testExactRoutes() {
    assertTrue(catalog.isRegistered("api_v1_clusterState"));
    assertTrue(catalog.isRegistered("api_v1_containers"));
    assertTrue(catalog.isRegistered("api_v1_keys_listKeys"));
    assertTrue(catalog.isRegistered("api_v1_containers_unhealthy_state"));
  }

  @Test
  public void testUnregisteredRoutes() {
    assertFalse(catalog.isRegistered(null));
    assertFalse(catalog.isRegistered(""));
    assertFalse(catalog.isRegistered("api_v1_unknown"));
    assertFalse(catalog.isRegistered("api_v1_keys2"));
    assertFalse(catalog.isRegistered("/api/v1/clusterState")); // old path format should be rejected
  }
}
