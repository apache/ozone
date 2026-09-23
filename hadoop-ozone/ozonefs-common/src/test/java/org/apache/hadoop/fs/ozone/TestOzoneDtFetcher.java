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

package org.apache.hadoop.fs.ozone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import java.io.IOException;
import java.net.URI;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.DtFetcher;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

/**
 * Tests Ozone delegation token fetchers.
 */
public class TestOzoneDtFetcher {
  private static final String RENEWER = "renewer";

  @ParameterizedTest
  @MethodSource("fetchers")
  public void fetchesTokenFromExpectedFileSystem(DtFetcher fetcher,
      String url, URI expectedUri) throws Exception {
    Configuration conf = new OzoneConfiguration();
    Credentials creds = new Credentials();
    FileSystem fs = mock(FileSystem.class);
    Token<?> token = new Token<>();
    Text service = new Text("om-service");
    token.setService(service);
    doReturn(token).when(fs).getDelegationToken(RENEWER);

    try (MockedStatic<FileSystem> fileSystems = mockStatic(FileSystem.class)) {
      fileSystems.when(() -> FileSystem.get(expectedUri, conf)).thenReturn(fs);

      assertSame(token,
          fetcher.addDelegationTokens(conf, creds, RENEWER, url));
      assertSame(token, creds.getToken(service));
      fileSystems.verify(() -> FileSystem.get(expectedUri, conf));
    }
  }

  private static Stream<Arguments> fetchers() {
    return Stream.of(
        Arguments.of(new O3fsDtFetcher(),
            "o3fs://bucket.volume.om/key",
            URI.create("o3fs://bucket.volume.om/key")),
        Arguments.of(new OfsDtFetcher(),
            "ofs://om/volume/bucket/key",
            URI.create("ofs://om/volume/bucket/key")),
        Arguments.of(new O3DtFetcher(),
            "o3://om-service/volume/bucket/key",
            URI.create("ofs://om-service/")),
        Arguments.of(new O3DtFetcher(),
            "om-service/volume/bucket/key",
            URI.create("ofs://om-service/")),
        Arguments.of(new O3DtFetcher(),
            "o3://om:9862/volume/bucket/key?query#fragment",
            URI.create("ofs://om:9862/")));
  }

  @Test
  public void checksFullServiceNamePrefix() throws Exception {
    AbstractOzoneDtFetcher fetcher = new AbstractOzoneDtFetcher() {
      @Override
      public Text getServiceName() {
        return new Text("o3");
      }

      @Override
      protected Token<?> addDelegationTokens(Configuration conf,
          Credentials creds, String renewer, URI uri) {
        assertEquals(URI.create("o3://o3fs://bucket.volume.om/key"), uri);
        return null;
      }
    };

    fetcher.addDelegationTokens(new OzoneConfiguration(), new Credentials(),
        RENEWER, "o3fs://bucket.volume.om/key");
  }

  @Test
  public void hasExpectedServiceNames() {
    assertEquals(new Text("o3fs"), new O3fsDtFetcher().getServiceName());
    assertEquals(new Text("ofs"), new OfsDtFetcher().getServiceName());
    assertEquals(new Text("o3"), new O3DtFetcher().getServiceName());
  }

  @Test
  public void rejectsO3UrlWithoutAuthority() {
    O3DtFetcher fetcher = new O3DtFetcher();
    assertThrows(IllegalArgumentException.class,
        () -> fetcher.addDelegationTokens(new OzoneConfiguration(),
            new Credentials(), RENEWER, "o3:///"));
  }

  @Test
  public void rejectsNullToken() throws Exception {
    Configuration conf = new OzoneConfiguration();
    Credentials creds = new Credentials();
    FileSystem fs = mock(FileSystem.class);
    URI uri = URI.create("ofs://om/");
    doReturn(null).when(fs).getDelegationToken(RENEWER);

    try (MockedStatic<FileSystem> fileSystems = mockStatic(FileSystem.class)) {
      fileSystems.when(() -> FileSystem.get(uri, conf)).thenReturn(fs);
      assertThrows(IOException.class,
          () -> new OfsDtFetcher().addDelegationTokens(
              conf, creds, RENEWER, uri.toString()));
    }
  }
}
