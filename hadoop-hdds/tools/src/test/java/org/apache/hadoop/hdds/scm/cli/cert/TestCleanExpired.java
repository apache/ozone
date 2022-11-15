package org.apache.hadoop.hdds.scm.cli.cert;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
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

public class TestCleanExpired {

  private CleanExpired cmd;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();


  @Mock
  DBStore dbStore;
  @Mock
  Table<BigInteger, X509Certificate> mockTable;
  @Mock
  TableIterator iterator;
  @Mock
  Table.KeyValue<BigInteger, X509Certificate> kv;
  @Mock
  Table.KeyValue<BigInteger, X509Certificate> kv2;
  @Mock
  X509Certificate nonExpiredCert;
  @Mock
  X509Certificate expiredCert;


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
    OzoneConfiguration conf = new OzoneConfiguration();
    // Mockito.when(dbStore.getTable("validCerts", BigInteger.class, X509Certificate.class)).thenReturn(mockTable);
    Mockito.when(SCMDBDefinition.VALID_CERTS.getTable(dbStore)).thenReturn(mockTable);
    TableIterator<BigInteger, ? extends Table.KeyValue<BigInteger, X509Certificate>> iterator1 = mockTable.iterator();
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