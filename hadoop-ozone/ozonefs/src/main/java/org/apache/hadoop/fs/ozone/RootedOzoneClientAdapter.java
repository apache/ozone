/**
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
package org.apache.hadoop.fs.ozone;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.token.Token;

/**
 * Lightweight adapter to separate hadoop/ozone classes.
 * <p>
 * This class contains only the bare minimum Ozone classes in the signature.
 * It could be loaded by a different classloader because only the objects in
 * the method signatures should be shared between the classloader.
 */
public interface RootedOzoneClientAdapter {

  void close() throws IOException;

  InputStream readFile(String pathStr) throws IOException;

  OzoneFSOutputStream createFile(String pathStr, boolean overWrite,
      boolean recursive) throws IOException;

  void rename(String pathStr, String newPath) throws IOException;

  boolean createDirectory(String pathStr) throws IOException;

  boolean deleteObject(String pathStr);

  Iterator<BasicKeyInfo> listKeys(String pathStr) throws IOException;

  List<FileStatusAdapter> listStatus(String pathStr, boolean recursive,
      String startPath, long numEntries, URI uri,
      Path workingDir, String username) throws IOException;

  FileStatusAdapter getFileStatus(String pathStr, URI uri,
      Path qualifiedPath, String userName) throws IOException;

  Token<OzoneTokenIdentifier> getDelegationToken(String renewer)
      throws IOException;

  KeyProvider getKeyProvider() throws IOException;

  URI getKeyProviderUri() throws IOException;

  String getCanonicalServiceName();

}
