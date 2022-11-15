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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.concurrent.Callable;

/**
 * This is the handler to clean SCM database from expired certificates
 */
@CommandLine.Command(
    name = "clean",
    description = "Clean expired certificates from the SCM metadata",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class CleanExpired implements Callable<Void>, SubcommandWithParent {

  @CommandLine.Option(names = {"--db"},
      required = true,
      description = "Database metadata directory")
  private String dbDirectory;

  @CommandLine.Option(names = {"--name"},
      required = true,
      description = "Database file name")
  private String dbName;

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @Override
  public Void call() {
    GenericParentCommand parent =
        (GenericParentCommand) spec.root().userObject();

    OzoneConfiguration configuration = parent.createOzoneConfiguration();

    if (!Files.exists(Paths.get(dbDirectory))) {
      System.out.println("DB path not exist:" + dbDirectory);
      return null;
    }

    //DBStore mockolás, inkább ezt adjuk a removeExpCertnek ++ hibakezelés (nincs certstable)
    try (DBStore dbStore = HAUtils.loadDB(
        configuration, new File(dbDirectory), dbName, new SCMDBDefinition())) {
      removeExpiredCertificates(dbStore);
    } catch (IOException e) {
      System.out.println("Error trying to open" + dbName +
          " at: " + dbDirectory);
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
      System.out.println("Error when trying to open certificate table from db");
    }
  }

  @Override
  public Class<?> getParentType() {
    return CertCommands.class;
  }
}
