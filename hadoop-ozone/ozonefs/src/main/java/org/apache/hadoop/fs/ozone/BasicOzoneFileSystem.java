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

package org.apache.hadoop.fs.ozone;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

import com.google.common.base.Preconditions;
import static org.apache.hadoop.fs.ozone.Constants.LISTING_PAGE_SIZE;
import static org.apache.hadoop.fs.ozone.Constants.OZONE_DEFAULT_USER;
import static org.apache.hadoop.fs.ozone.Constants.OZONE_USER_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The minimal Ozone Filesystem implementation.
 * <p>
 * This is a basic version which doesn't extend
 * KeyProviderTokenIssuer and doesn't include statistics. It can be used
 * from older hadoop version. For newer hadoop version use the full featured
 * OzoneFileSystem.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BasicOzoneFileSystem extends FileSystem {
  static final Logger LOG =
      LoggerFactory.getLogger(BasicOzoneFileSystem.class);

  /**
   * The Ozone client for connecting to Ozone server.
   */

  private URI uri;
  private String userName;
  private Path workingDir;

  private OzoneClientAdapter adapter;

  private static final Pattern URL_SCHEMA_PATTERN =
      Pattern.compile("([^\\.]+)\\.([^\\.]+)\\.{0,1}(.*)");

  private static final String URI_EXCEPTION_TEXT = "Ozone file system URL " +
      "should be one of the following formats: " +
      "o3fs://bucket.volume/key  OR " +
      "o3fs://bucket.volume.om-host.example.com/key  OR " +
      "o3fs://bucket.volume.om-host.example.com:5678/key";

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    setConf(conf);
    Objects.requireNonNull(name.getScheme(), "No scheme provided in " + name);
    Preconditions.checkArgument(getScheme().equals(name.getScheme()),
        "Invalid scheme provided in " + name);

    String authority = name.getAuthority();
    if (authority == null) {
      // authority is null when fs.defaultFS is not a qualified o3fs URI and
      // o3fs:/// is passed to the client. matcher will NPE if authority is null
      throw new IllegalArgumentException(URI_EXCEPTION_TEXT);
    }

    Matcher matcher = URL_SCHEMA_PATTERN.matcher(authority);

    if (!matcher.matches()) {
      throw new IllegalArgumentException(URI_EXCEPTION_TEXT);
    }
    String bucketStr = matcher.group(1);
    String volumeStr = matcher.group(2);
    String remaining = matcher.groupCount() == 3 ? matcher.group(3) : null;

    String omHost = null;
    int omPort = -1;
    if (!isEmpty(remaining)) {
      String[] parts = remaining.split(":");
      // Array length should be either 1(hostname or service id) or 2(host:port)
      if (parts.length > 2) {
        throw new IllegalArgumentException(URI_EXCEPTION_TEXT);
      }
      omHost = parts[0];
      if (parts.length == 2) {
        try {
          omPort = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(URI_EXCEPTION_TEXT);
        }
      }
    }

    try {
      uri = new URIBuilder().setScheme(OZONE_URI_SCHEME)
          .setHost(authority)
          .build();
      LOG.trace("Ozone URI for ozfs initialization is {}", uri);

      //isolated is the default for ozonefs-lib-legacy which includes the
      // /ozonefs.txt, otherwise the default is false. It could be overridden.
      boolean defaultValue =
          BasicOzoneFileSystem.class.getClassLoader()
              .getResource("ozonefs.txt")
              != null;

      //Use string here instead of the constant as constant may not be available
      //on the classpath of a hadoop 2.7
      boolean isolatedClassloader =
          conf.getBoolean("ozone.fs.isolated-classloader", defaultValue);

      ConfigurationSource source;
      if (conf instanceof OzoneConfiguration) {
        source = (ConfigurationSource) conf;
      } else {
        source = new LegacyHadoopConfigurationSource(conf);
      }
      this.adapter =
          createAdapter(source, bucketStr,
              volumeStr, omHost, omPort,
          isolatedClassloader);

      try {
        this.userName =
            UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException e) {
        this.userName = OZONE_DEFAULT_USER;
      }
      this.workingDir = new Path(OZONE_USER_DIR, this.userName)
          .makeQualified(this.uri, this.workingDir);

    } catch (URISyntaxException ue) {
      final String msg = "Invalid Ozone endpoint " + name;
      LOG.error(msg, ue);
      throw new IOException(msg, ue);
    }
  }

  protected OzoneClientAdapter createAdapter(ConfigurationSource conf,
      String bucketStr,
      String volumeStr, String omHost, int omPort,
      boolean isolatedClassloader) throws IOException {

    if (isolatedClassloader) {

      return OzoneClientAdapterFactory
          .createAdapter(volumeStr, bucketStr);

    } else {

      return new BasicOzoneClientAdapterImpl(omHost, omPort, conf,
          volumeStr, bucketStr);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      adapter.close();
    } finally {
      super.close();
    }
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public String getScheme() {
    return OZONE_URI_SCHEME;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    incrementCounter(Statistic.INVOCATION_OPEN);
    statistics.incrementReadOps(1);
    LOG.trace("open() path:{}", f);
    final String key = pathToKey(f);
    InputStream inputStream = adapter.readFile(key);
    return new FSDataInputStream(createFSInputStream(inputStream));
  }

  protected InputStream createFSInputStream(InputStream inputStream) {
    return new OzoneFSInputStream(inputStream, statistics);
  }

  protected void incrementCounter(Statistic statistic) {
    //don't do anyting in this default implementation.
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize,
      short replication, long blockSize,
      Progressable progress) throws IOException {
    LOG.trace("create() path:{}", f);
    incrementCounter(Statistic.INVOCATION_CREATE);
    statistics.incrementWriteOps(1);
    final String key = pathToKey(f);
    return createOutputStream(key, replication, overwrite, true);
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path path,
      FsPermission permission,
      EnumSet<CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    incrementCounter(Statistic.INVOCATION_CREATE_NON_RECURSIVE);
    statistics.incrementWriteOps(1);
    final String key = pathToKey(path);
    return createOutputStream(key,
        replication, flags.contains(CreateFlag.OVERWRITE), false);
  }

  private FSDataOutputStream createOutputStream(String key, short replication,
      boolean overwrite, boolean recursive) throws IOException {
    return new FSDataOutputStream(adapter.createFile(key,
        replication, overwrite, recursive), statistics);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new UnsupportedOperationException("append() Not implemented by the "
        + getClass().getSimpleName() + " FileSystem implementation");
  }

  /**
   * Check whether the source and destination path are valid and then perform
   * rename from source path to destination path.
   * <p>
   * The rename operation is performed by renaming the keys with src as prefix.
   * For such keys the prefix is changed from src to dst.
   *
   * @param src source path for rename
   * @param dst destination path for rename
   * @return true if rename operation succeeded or
   * if the src and dst have the same path and are of the same type
   * @throws IOException on I/O errors or if the src/dst paths are invalid.
   */
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    incrementCounter(Statistic.INVOCATION_RENAME);
    statistics.incrementWriteOps(1);
    super.checkPath(src);
    super.checkPath(dst);

    String srcPath = src.toUri().getPath();
    String dstPath = dst.toUri().getPath();
    if (srcPath.equals(dstPath)) {
      return true;
    }

    LOG.trace("rename() from:{} to:{}", src, dst);
    if (src.isRoot()) {
      // Cannot rename root of file system
      LOG.trace("Cannot rename the root of a filesystem");
      return false;
    }

    if (dst.isRoot()) {
      // Cannot rename root of file system
      throw new IOException("rename destination cannot be the root");
    }

    adapter.renameKey(srcPath, dstPath);
    return true;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    incrementCounter(Statistic.INVOCATION_DELETE);
    statistics.incrementWriteOps(1);
    LOG.debug("Delete path {} - recursive {}", f, recursive);

    String key = pathToKey(f);
    if (key.equals("")) {
      LOG.trace("Skipping deleting root directory");
      return false;
    }
    LOG.debug("delete path : {}", f);
    boolean result = adapter.deleteObject(key);

    if (result) {
      // If this delete operation removes all files/directories from the
      // parent directory, then an empty parent directory must be created.
      createFakeParentDirectory(f);
    }

    return result;
  }

  /**
   * Create a fake parent directory key if it does not already exist and no
   * other child of this parent directory exists.
   *
   * @param f path to the fake parent directory
   * @throws IOException
   */
  private void createFakeParentDirectory(Path f) throws IOException {
    Path parent = f.getParent();
    if (parent != null && !parent.isRoot()) {
      createFakeDirectoryIfNecessary(parent);
    }
  }

  /**
   * Create a fake directory key if it does not already exist.
   *
   * @param f path to the fake directory
   * @throws IOException
   */
  private void createFakeDirectoryIfNecessary(Path f) throws IOException {
    String key = pathToKey(f);
    if (!key.isEmpty() && !o3Exists(f)) {
      LOG.debug("Creating new fake directory at {}", f);
      String dirKey = addTrailingSlashIfNeeded(key);
      adapter.createDirectory(dirKey);
    }
  }

  /**
   * Check if a file or directory exists corresponding to given path.
   *
   * @param f path to file/directory.
   * @return true if it exists, false otherwise.
   * @throws IOException
   */
  private boolean o3Exists(final Path f) throws IOException {
    Path path = makeQualified(f);
    try {
      getFileStatus(path);
      return true;
    } catch (FileNotFoundException ex) {
      return false;
    }
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    incrementCounter(Statistic.INVOCATION_LIST_STATUS);
    statistics.incrementReadOps(1);
    LOG.trace("listStatus() path:{}", f);
    int numEntries = LISTING_PAGE_SIZE;
    LinkedList<FileStatus> statuses = new LinkedList<>();
    List<FileStatus> tmpStatusList;
    String startKey = "";

    do {
      tmpStatusList =
          adapter.listStatus(pathToKey(f), false, startKey, numEntries, uri,
              workingDir, getUsername())
              .stream()
              .map(this::convertFileStatus)
              .collect(Collectors.toList());

      if (!tmpStatusList.isEmpty()) {
        if (startKey.isEmpty()) {
          statuses.addAll(tmpStatusList);
        } else {
          statuses.addAll(tmpStatusList.subList(1, tmpStatusList.size()));
        }
        startKey = pathToKey(statuses.getLast().getPath());
      }
      // listStatus returns entries numEntries in size if available.
      // Any lesser number of entries indicate that the required entries have
      // exhausted.
    } while (tmpStatusList.size() == numEntries);


    return statuses.toArray(new FileStatus[0]);
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDir = newDir;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public Token<?> getDelegationToken(String renewer) throws IOException {
    return adapter.getDelegationToken(renewer);
  }

  /**
   * Get a canonical service name for this file system. If the URI is logical,
   * the hostname part of the URI will be returned.
   *
   * @return a service string that uniquely identifies this file system.
   */
  @Override
  public String getCanonicalServiceName() {
    return adapter.getCanonicalServiceName();
  }

  /**
   * Get the username of the FS.
   *
   * @return the short name of the user who instantiated the FS
   */
  public String getUsername() {
    return userName;
  }

  /**
   * Creates a directory. Directory is represented using a key with no value.
   *
   * @param path directory path to be created
   * @return true if directory exists or created successfully.
   * @throws IOException
   */
  private boolean mkdir(Path path) throws IOException {
    return adapter.createDirectory(pathToKey(path));
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    LOG.trace("mkdir() path:{} ", f);
    String key = pathToKey(f);
    if (isEmpty(key)) {
      return false;
    }
    return mkdir(f);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    incrementCounter(Statistic.INVOCATION_GET_FILE_STATUS);
    statistics.incrementReadOps(1);
    LOG.trace("getFileStatus() path:{}", f);
    Path qualifiedPath = f.makeQualified(uri, workingDir);
    String key = pathToKey(qualifiedPath);
    FileStatus fileStatus = null;
    try {
      fileStatus = convertFileStatus(
          adapter.getFileStatus(key, uri, qualifiedPath, getUsername()));
    } catch (OMException ex) {
      if (ex.getResult().equals(OMException.ResultCodes.KEY_NOT_FOUND)) {
        throw new FileNotFoundException("File not found. path:" + f);
      }
    }
    return fileStatus;
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus fileStatus,
                                               long start, long len)
      throws IOException {
    if (fileStatus instanceof LocatedFileStatus) {
      return ((LocatedFileStatus) fileStatus).getBlockLocations();
    } else {
      return super.getFileBlockLocations(fileStatus, start, len);
    }
  }

  @Override
  public short getDefaultReplication() {
    return adapter.getDefaultReplication();
  }

  /**
   * Turn a path (relative or otherwise) into an Ozone key.
   *
   * @param path the path of the file.
   * @return the key of the object that represents the file.
   */
  public String pathToKey(Path path) {
    Objects.requireNonNull(path, "Path can not be null!");
    if (!path.isAbsolute()) {
      path = new Path(workingDir, path);
    }
    // removing leading '/' char
    String key = path.toUri().getPath().substring(1);
    LOG.trace("path for key:{} is:{}", key, path);
    return key;
  }

  /**
   * Add trailing delimiter to path if it is already not present.
   *
   * @param key the ozone Key which needs to be appended
   * @return delimiter appended key
   */
  private String addTrailingSlashIfNeeded(String key) {
    if (!isEmpty(key) && !key.endsWith(OZONE_URI_DELIMITER)) {
      return key + OZONE_URI_DELIMITER;
    } else {
      return key;
    }
  }

  @Override
  public String toString() {
    return "OzoneFileSystem{URI=" + uri + ", "
        + "workingDir=" + workingDir + ", "
        + "userName=" + userName + ", "
        + "statistics=" + statistics
        + "}";
  }

  public OzoneClientAdapter getAdapter() {
    return adapter;
  }

  public boolean isEmpty(CharSequence cs) {
    return cs == null || cs.length() == 0;
  }

  public boolean isNumber(String number) {
    try {
      Integer.parseInt(number);
    } catch (NumberFormatException ex) {
      return false;
    }
    return true;
  }

  private FileStatus convertFileStatus(
      FileStatusAdapter fileStatusAdapter) {

    Path symLink = null;
    try {
      fileStatusAdapter.getSymlink();
    } catch (Exception ex) {
      //NOOP: If not symlink symlink remains null.
    }

    FileStatus fileStatus =  new FileStatus(
        fileStatusAdapter.getLength(),
        fileStatusAdapter.isDir(),
        fileStatusAdapter.getBlockReplication(),
        fileStatusAdapter.getBlocksize(),
        fileStatusAdapter.getModificationTime(),
        fileStatusAdapter.getAccessTime(),
        new FsPermission(fileStatusAdapter.getPermission()),
        fileStatusAdapter.getOwner(),
        fileStatusAdapter.getGroup(),
        symLink,
        fileStatusAdapter.getPath()
    );

    BlockLocation[] blockLocations = fileStatusAdapter.getBlockLocations();
    if (blockLocations == null || blockLocations.length == 0) {
      return fileStatus;
    }
    return new LocatedFileStatus(fileStatus, blockLocations);
  }

}
