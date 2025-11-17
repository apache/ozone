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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.ozone.OzoneFsServerDefaults;
import org.apache.hadoop.ozone.om.helpers.LeaseKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.token.Token;

/**
 * Lightweight adapter to separate hadoop/ozone classes.
 * <p>
 * This class contains only the bare minimum Ozone classes in the signature.
 * It could be loaded by a different classloader because only the objects in
 * the method signatures should be shared between the classloader.
 */
public interface OzoneClientAdapter {

  void close() throws IOException;

  InputStream readFile(String key) throws IOException;

  OzoneFSOutputStream createFile(String key, short replication,
      boolean overWrite, boolean recursive) throws IOException;

  OzoneFSDataStreamOutput createStreamFile(String key, short replication,
      boolean overWrite, boolean recursive) throws IOException;

  void renameKey(String key, String newKeyName) throws IOException;

  // Users should use rename instead of renameKey in OFS.
  void rename(String pathStr, String newPath) throws IOException;

  boolean createDirectory(String keyName) throws IOException;

  boolean deleteObject(String keyName) throws IOException;

  boolean deleteObject(String keyName, boolean recursive) throws IOException;

  boolean deleteObjects(List<String> keyName);

  Iterator<BasicKeyInfo> listKeys(String pathKey) throws IOException;

  @SuppressWarnings("checkstyle:ParameterNumber")
  List<FileStatusAdapter> listStatus(String keyName, boolean recursive,
      String startKey, long numEntries, URI uri,
      Path workingDir, String username, boolean lite) throws IOException;

  Token<OzoneTokenIdentifier> getDelegationToken(String renewer)
      throws IOException;
  
  OzoneFsServerDefaults getServerDefaults() throws IOException;

  KeyProvider getKeyProvider() throws IOException;

  URI getKeyProviderUri() throws IOException;

  String getCanonicalServiceName();

  short getDefaultReplication();

  FileStatusAdapter getFileStatus(String key, URI uri,
      Path qualifiedPath, String userName) throws IOException;

  boolean isFSOptimizedBucket();

  FileChecksum getFileChecksum(String keyName, long length) throws IOException;

  String createSnapshot(String pathStr, String snapshotName) throws IOException;

  void renameSnapshot(String pathStr, String snapshotOldName, String snapshotNewName) throws IOException;

  void deleteSnapshot(String pathStr, String snapshotName) throws IOException;

  SnapshotDiffReport getSnapshotDiffReport(Path snapshotDir,
      String fromSnapshot, String toSnapshot)
      throws IOException, InterruptedException;

  LeaseKeyInfo recoverFilePrepare(String pathStr, boolean force) throws IOException;

  void recoverFile(OmKeyArgs keyArgs) throws IOException;

  long finalizeBlock(OmKeyLocationInfo block) throws IOException;

  void setTimes(String key, long mtime, long atime) throws IOException;

  boolean isFileClosed(String pathStr) throws IOException;

  boolean setSafeMode(SafeModeAction action, boolean isChecked)
      throws IOException;
}
