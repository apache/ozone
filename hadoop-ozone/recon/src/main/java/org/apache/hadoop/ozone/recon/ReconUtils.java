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

package org.apache.hadoop.ozone.recon;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.zip.GZIPOutputStream;

import com.google.inject.Singleton;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.io.IOUtils;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import static org.apache.hadoop.hdds.server.ServerUtils.getDirectoryFromConfig;
import static org.apache.hadoop.hdds.server.ServerUtils.getOzoneMetaDirPath;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_DB_DIR;
import static org.jooq.impl.DSL.currentTimestamp;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.using;

import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
import org.hadoop.ozone.recon.schema.tables.pojos.GlobalStats;
import org.jooq.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon Utility class.
 */
@Singleton
public class ReconUtils {

  private static final int WRITE_BUFFER = 1048576; //1MB

  public ReconUtils() {
  }

  private static final Logger LOG = LoggerFactory.getLogger(
      ReconUtils.class);

  public static File getReconScmDbDir(ConfigurationSource conf) {
    return new ReconUtils().getReconDbDir(conf, OZONE_RECON_SCM_DB_DIR);
  }

  /**
   * Get configured Recon DB directory value based on config. If not present,
   * fallback to ozone.metadata.dirs
   *
   * @param conf         configuration bag
   * @param dirConfigKey key to check
   * @return Return File based on configured or fallback value.
   */
  public File getReconDbDir(ConfigurationSource conf, String dirConfigKey) {

    File metadataDir = getDirectoryFromConfig(conf, dirConfigKey,
        "Recon");
    if (metadataDir != null) {
      return metadataDir;
    }

    LOG.warn("{} is not configured. We recommend adding this setting. " +
            "Falling back to {} instead.",
        dirConfigKey, HddsConfigKeys.OZONE_METADATA_DIRS);
    return getOzoneMetaDirPath(conf);
  }

  /**
   * Given a source directory, create a tar.gz file from it.
   *
   * @param sourcePath the path to the directory to be archived.
   * @return tar.gz file
   * @throws IOException
   */
  public static File createTarFile(Path sourcePath) throws IOException {
    TarArchiveOutputStream tarOs = null;
    FileOutputStream fileOutputStream = null;
    GZIPOutputStream gzipOutputStream = null;
    try {
      String sourceDir = sourcePath.toString();
      String fileName = sourceDir.concat(".tar.gz");
      fileOutputStream = new FileOutputStream(fileName);
      gzipOutputStream =
          new GZIPOutputStream(new BufferedOutputStream(fileOutputStream));
      tarOs = new TarArchiveOutputStream(gzipOutputStream);
      File folder = new File(sourceDir);
      File[] filesInDir = folder.listFiles();
      if (filesInDir != null) {
        for (File file : filesInDir) {
          addFilesToArchive(file.getName(), file, tarOs);
        }
      }
      return new File(fileName);
    } finally {
      try {
        org.apache.hadoop.io.IOUtils.closeStream(tarOs);
        org.apache.hadoop.io.IOUtils.closeStream(fileOutputStream);
        org.apache.hadoop.io.IOUtils.closeStream(gzipOutputStream);
      } catch (Exception e) {
        LOG.error("Exception encountered when closing " +
            "TAR file output stream: " + e);
      }
    }
  }

  private static void addFilesToArchive(String source, File file,
                                        TarArchiveOutputStream
                                            tarFileOutputStream)
      throws IOException {
    tarFileOutputStream.putArchiveEntry(new TarArchiveEntry(file, source));
    if (file.isFile()) {
      try (FileInputStream fileInputStream = new FileInputStream(file)) {
        BufferedInputStream bufferedInputStream =
            new BufferedInputStream(fileInputStream);
        org.apache.commons.compress.utils.IOUtils.copy(bufferedInputStream,
            tarFileOutputStream);
        tarFileOutputStream.closeArchiveEntry();
      }
    } else if (file.isDirectory()) {
      tarFileOutputStream.closeArchiveEntry();
      File[] filesInDir = file.listFiles();
      if (filesInDir != null) {
        for (File cFile : filesInDir) {
          addFilesToArchive(cFile.getAbsolutePath(), cFile,
              tarFileOutputStream);
        }
      }
    }
  }

  /**
   * Untar DB snapshot tar file to recon OM snapshot directory.
   *
   * @param tarFile  source tar file
   * @param destPath destination path to untar to.
   * @throws IOException ioException
   */
  public void untarCheckpointFile(File tarFile, Path destPath)
      throws IOException {

    FileInputStream fileInputStream = null;
    BufferedInputStream buffIn = null;
    GzipCompressorInputStream gzIn = null;
    try {
      fileInputStream = new FileInputStream(tarFile);
      buffIn = new BufferedInputStream(fileInputStream);
      gzIn = new GzipCompressorInputStream(buffIn);

      //Create Destination directory if it does not exist.
      if (!destPath.toFile().exists()) {
        boolean success = destPath.toFile().mkdirs();
        if (!success) {
          throw new IOException("Unable to create Destination directory.");
        }
      }

      try (TarArchiveInputStream tarInStream =
               new TarArchiveInputStream(gzIn)) {
        TarArchiveEntry entry;

        while ((entry = (TarArchiveEntry) tarInStream.getNextEntry()) != null) {
          Path path = Paths.get(destPath.toString(), entry.getName());
          HddsUtils.validatePath(path, destPath);
          File f = path.toFile();
          //If directory, create a directory.
          if (entry.isDirectory()) {
            boolean success = f.mkdirs();
            if (!success) {
              LOG.error("Unable to create directory found in tar.");
            }
          } else {
            //Write contents of file in archive to a new file.
            int count;
            byte[] data = new byte[WRITE_BUFFER];

            FileOutputStream fos = new FileOutputStream(f);
            try (BufferedOutputStream dest =
                     new BufferedOutputStream(fos, WRITE_BUFFER)) {
              while ((count =
                  tarInStream.read(data, 0, WRITE_BUFFER)) != -1) {
                dest.write(data, 0, count);
              }
            }
          }
        }
      }
    } finally {
      IOUtils.closeStream(gzIn);
      IOUtils.closeStream(buffIn);
      IOUtils.closeStream(fileInputStream);
    }
  }

  /**
   * Make HTTP GET call on the URL and return HttpURLConnection instance.
   * @param connectionFactory URLConnectionFactory to use.
   * @param url url to call
   * @param isSpnego is SPNEGO enabled
   * @return HttpURLConnection instance of the HTTP call.
   * @throws IOException, AuthenticationException While reading the response.
   */
  public HttpURLConnection makeHttpCall(URLConnectionFactory connectionFactory,
                                  String url, boolean isSpnego)
      throws IOException, AuthenticationException {
    HttpURLConnection urlConnection = (HttpURLConnection)
          connectionFactory.openConnection(new URL(url), isSpnego);
    urlConnection.connect();
    return urlConnection;
  }

  /**
   * Load last known DB in Recon.
   * @param reconDbDir
   * @param fileNamePrefix
   * @return
   */
  public File getLastKnownDB(File reconDbDir, String fileNamePrefix) {
    String lastKnownSnapshotFileName = null;
    long lastKnonwnSnapshotTs = Long.MIN_VALUE;
    if (reconDbDir != null) {
      File[] snapshotFiles = reconDbDir.listFiles((dir, name) ->
          name.startsWith(fileNamePrefix));
      if (snapshotFiles != null) {
        for (File snapshotFile : snapshotFiles) {
          String fileName = snapshotFile.getName();
          try {
            String[] fileNameSplits = fileName.split("_");
            if (fileNameSplits.length <= 1) {
              continue;
            }
            long snapshotTimestamp = Long.parseLong(fileNameSplits[1]);
            if (lastKnonwnSnapshotTs < snapshotTimestamp) {
              lastKnonwnSnapshotTs = snapshotTimestamp;
              lastKnownSnapshotFileName = fileName;
            }
          } catch (NumberFormatException nfEx) {
            LOG.warn("Unknown file found in Recon DB dir : {}", fileName);
          }
        }
      }
    }
    return lastKnownSnapshotFileName == null ? null :
        new File(reconDbDir.getPath(), lastKnownSnapshotFileName);
  }

  /**
   * Upsert row in GlobalStats table.
   *
   * @param sqlConfiguration
   * @param globalStatsDao
   * @param key
   * @param count
   */
  public static void upsertGlobalStatsTable(Configuration sqlConfiguration,
                                            GlobalStatsDao globalStatsDao,
                                            String key,
                                            Long count) {
    // Get the current timestamp
    Timestamp now =
        using(sqlConfiguration).fetchValue(select(currentTimestamp()));
    GlobalStats record = globalStatsDao.fetchOneByKey(key);
    GlobalStats newRecord = new GlobalStats(key, count, now);

    // Insert a new record for key if it does not exist
    if (record == null) {
      globalStatsDao.insert(newRecord);
    } else {
      globalStatsDao.update(newRecord);
    }
  }
}
