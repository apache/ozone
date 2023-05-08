/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.s3.remote.vault;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.api.Logical;
import com.bettercloud.vault.response.LogicalResponse;
import com.bettercloud.vault.response.LookupResponse;
import com.bettercloud.vault.rest.RestResponse;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.s3.remote.vault.auth.Auth;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for {@link VaultS3SecretStore}.
 */
public class VaultS3SecretStoreTest {
  private static final String TOKEN = "token";
  private static final AtomicInteger FAIL_AUTH_COUNTER = new AtomicInteger(0);
  private static final Map<String, Map<String, String>> STORE = new HashMap<>();
  private static VaultConfig config;
  private static VaultS3SecretStore s3SecretStore;

  @BeforeAll
  static void setup() throws IOException {
    Vault vault = mock(Vault.class);

    Auth auth = config1 -> {
      config1.token(TOKEN);
      config = config1;
      return vault;
    };

    s3SecretStore = new VaultS3SecretStore(
        "local",
        "namespace",
        "secretPath",
        1,
        auth,
        null);

    when(vault.logical()).thenReturn(new LogicalMock());
    when(vault.auth()).thenReturn(new AuthMock());
  }

  @BeforeEach
  public void clean() {
    STORE.clear();
  }

  @Test
  public void testReadWrite() throws IOException {
    S3SecretValue secret = new S3SecretValue("id", "value");
    s3SecretStore.storeSecret(
        "id",
        secret);

    assertEquals(secret, s3SecretStore.getSecret("id"));
  }

  @Test
  public void testReAuth() throws IOException {
    S3SecretValue secret = new S3SecretValue("id", "value");
    s3SecretStore.storeSecret("id", secret);

    FAIL_AUTH_COUNTER.set(1);

    assertEquals(secret, s3SecretStore.getSecret("id"));

    FAIL_AUTH_COUNTER.set(1);

    assertDoesNotThrow(() -> s3SecretStore.revokeSecret("id"));
  }

  @Test
  public void testAuthFail() throws IOException {
    S3SecretValue secret = new S3SecretValue("id", "value");
    s3SecretStore.storeSecret("id", secret);

    FAIL_AUTH_COUNTER.set(2);

    assertThrows(IOException.class,
        () -> s3SecretStore.getSecret("id"));

    FAIL_AUTH_COUNTER.set(2);

    assertThrows(IOException.class,
        () -> s3SecretStore.revokeSecret("id"));
  }

  private static class LogicalMock extends Logical {
    LogicalMock() {
      super(config);
    }

    @Override
    public LogicalResponse read(String path) {
      return new LogicalResponseMock(path);
    }

    @Override
    public LogicalResponse write(String path,
                                 Map<String, Object> nameValuePairs) {
      STORE.put(path, (Map) nameValuePairs);
      return new LogicalResponseMock(path);
    }

    @Override
    public LogicalResponse delete(String path) {
      STORE.remove(path);
      return new LogicalResponseMock(path);
    }
  }

  private static class AuthMock extends com.bettercloud.vault.api.Auth {
    AuthMock() {
      super(config);
    }

    @Override
    public LookupResponse lookupSelf() throws VaultException {
      if (FAIL_AUTH_COUNTER.getAndDecrement() == 0) {
        throw new VaultException("Fail", 401);
      }
      return new LookupResponseMock(200);
    }
  }

  private static class LookupResponseMock extends LookupResponse {
    LookupResponseMock(int code) {
      super(new RestResponse(code, "application/json", new byte[0]), 1);
    }

    @Override
    public String getId() {
      return TOKEN;
    }
  }

  private static class LogicalResponseMock extends LogicalResponse {
    private final String key;

    LogicalResponseMock(String key) {
      this(key, 200);
    }

    LogicalResponseMock(String key, int code) {
      super(
          new RestResponse(code, "application/json", new byte[0]),
          1,
          Logical.logicalOperations.readV1);
      this.key = key;
    }

    @Override
    public Map<String, String> getData() {
      Map<String, String> result = STORE.get(key);
      return result == null ? Collections.emptyMap() : result;
    }
  }
}
