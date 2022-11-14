package org.apache.hadoop.hdds.scm.cli.cert;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;
import sun.security.x509.CertificateValidity;
import sun.security.x509.X509CertImpl;
import sun.security.x509.X509CertInfo;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.sql.Date;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCleanExpired {

  private CleanExpired cmd;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  @BeforeEach
  public void setup() throws IOException {
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
  public void testOnlyExpiredCertsRemoved(@TempDir Path tempDir)
      throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, tempDir.toString());
    File newFolder = new File(tempDir + "/newFolder");

    if (!newFolder.exists()) {
      assertTrue(newFolder.mkdirs());
    }
    DBDefinition sampleDBDef = new SCMDBDefinition();
    X509CertInfo certInfo = new X509CertInfo();
    certInfo.set(X509CertInfo.IDENT, new CertificateValidity(Date.from(Instant.now().minus(Duration.ofDays(365))), Date.from(Instant.now().plus(Duration.ofDays(365)))));
    X509Certificate nonExpired = new X509CertImpl(certInfo);
    certInfo.set(X509CertInfo.IDENT, new CertificateValidity(Date.from(Instant.now().minus(Duration.ofDays(365))), Date.from(Instant.now().minus(Duration.ofDays(15)))));
    X509Certificate expired = new X509CertImpl(certInfo);
    String dbName = "SampleStore";
    try (DBStore dbStore = DBStoreBuilder.newBuilder(conf, sampleDBDef)
        .setName("SampleStore").setPath(newFolder.toPath()).build()) {
      Table<BigInteger, X509Certificate> table = SCMDBDefinition.VALID_CERTS
          .getTable(dbStore);

      table.put(BigInteger.ONE, nonExpired);
      table.put(BigInteger.valueOf(2L), expired);

      CommandLine c = new CommandLine(cmd);
      c.parseArgs("-db", newFolder.getPath(), "-name", dbName);
      cmd.call();
      assertTrue(table.isExist(BigInteger.ONE));
      assertFalse(table.isExist(BigInteger.valueOf(2L)));
    }
    Assertions.assertFalse(false);
  }
}