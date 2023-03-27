/*
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
package org.apache.hadoop.hdds.scm.cli.cert;

import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.sql.Date;
import java.time.Duration;
import java.time.Instant;

/**
 * Test the cleaning tool for expired certificates.
 */
public class TestCleanExpired {

  private CleanExpired cmd;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  @Mock
  private DBStore dbStore;
  @Mock
  private Table<BigInteger, X509Certificate> mockTable;
  @Mock
  private TableIterator iterator;
  @Mock
  private Table.KeyValue<BigInteger, X509Certificate> kv;
  @Mock
  private Table.KeyValue<BigInteger, X509Certificate> kv2;
  @Mock
  private X509Certificate nonExpiredCert;
  @Mock
  private X509Certificate expiredCert;

  @BeforeEach
  public void setup() throws IOException {
    MockitoAnnotations.initMocks(this);
    cmd = new CleanExpired();
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testOnlyExpiredCertsRemoved()
      throws Exception {
    Mockito.when(SCMDBDefinition.VALID_CERTS.getTable(dbStore))
        .thenReturn(mockTable);
    Mockito.when(mockTable.iterator()).thenReturn(iterator);
    Mockito.when(nonExpiredCert.getNotAfter())
        .thenReturn(Date.from(Instant.now().plus(Duration.ofDays(365))));
    Mockito.when(expiredCert.getNotAfter())
        .thenReturn(Date.from(Instant.now().minus(Duration.ofDays(365))));
    Mockito.when(iterator.hasNext()).thenReturn(true, true, false);
    Mockito.when(iterator.next()).thenReturn(kv, kv2);
    Mockito.when(kv.getValue()).thenReturn(expiredCert);
    Mockito.when(kv2.getValue()).thenReturn(nonExpiredCert);

    cmd.removeExpiredCertificates(dbStore);
    Mockito.verify(iterator, Mockito.times(1)).removeFromDB();
  }
}
