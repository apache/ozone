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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.cli.GenericParentCommand;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.concurrent.Callable;

/**
 * This is the handler to clean SCM database from expired certificates.
 */
@CommandLine.Command(
    name = "clean",
    description = "Clean expired certificates from the SCM metadata. " +
        "This command is only supported when the SCM is shutdown.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class CleanExpired implements Callable<Void>, SubcommandWithParent {

  @CommandLine.Option(names = {"--db"},
      required = true,
      description = "Database file path")
  private String dbFilePath;

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @Override
  public Void call() {
    GenericParentCommand parent =
        (GenericParentCommand) spec.root().userObject();

    OzoneConfiguration configuration = parent.createOzoneConfiguration();

    File db = new File(dbFilePath);
    if (!db.exists()) {
      System.out.println("DB path does not exist: " + dbFilePath);
      return null;
    }

    try {
      DBStore dbStore = HAUtils.loadDB(
          configuration, db.getParentFile(),
          db.getName(), new SCMDBDefinition());
      removeExpiredCertificates(dbStore);
    } catch (Exception e) {

      System.out.println("Error trying to open file: " + dbFilePath +
          " failed with exception: " + e);
    }
    return null;
  }

  @VisibleForTesting
  void removeExpiredCertificates(DBStore dbStore) {
    try {
      Table<BigInteger, X509Certificate> certsTable =
          SCMDBDefinition.VALID_CERTS.getTable(dbStore);
      TableIterator<BigInteger, ? extends Table.KeyValue<BigInteger,
          X509Certificate>> tableIterator = certsTable.iterator();
      while (tableIterator.hasNext()) {
        Table.KeyValue<?, ?> certPair = tableIterator.next();
        X509Certificate certificate = (X509Certificate) certPair.getValue();
        if (Instant.now().isAfter(certificate.getNotAfter().toInstant())) {
          System.out.println("Certificate with id " + certPair.getKey() +
              " and value: " + certificate + "will be deleted");
          tableIterator.removeFromDB();
        }
      }
    } catch (IOException e) {
      System.out.println("Error when trying to open " +
          "certificate table from db: " + e);
    }
  }

  @Override
  public Class<?> getParentType() {
    return CertCommands.class;
  }
}
