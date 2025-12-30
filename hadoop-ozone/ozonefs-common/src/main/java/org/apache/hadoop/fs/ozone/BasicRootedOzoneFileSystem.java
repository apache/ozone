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

import static org.apache.hadoop.fs.ozone.Constants.OZONE_DEFAULT_USER;
import static org.apache.hadoop.fs.ozone.Constants.OZONE_USER_DIR;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_LISTING_PAGE_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_LISTING_PAGE_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_MAX_LISTING_PAGE_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_INDICATOR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_EMPTY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_EMPTY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import io.opentelemetry.api.trace.Span;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneFsServerDefaults;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.SelectorOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.apache.http.client.utils.URIBuilder;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The minimal Rooted Ozone Filesystem implementation.
 * <p>
 * This is a basic version which doesn't extend
 * KeyProviderTokenIssuer and doesn't include statistics. It can be used
 * from older hadoop version. For newer hadoop version use the full featured
 * RootedOzoneFileSystem.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BasicRootedOzoneFileSystem extends FileSystem {
  static final Logger LOG =
      LoggerFactory.getLogger(BasicRootedOzoneFileSystem.class);

  /**
   * The Ozone client for connecting to Ozone server.
   */

  private URI uri;
  private String userName;
  private Path workingDir;
  private OzoneClientAdapter adapter;
  private BasicRootedOzoneClientAdapterImpl adapterImpl;

  private int listingPageSize =
      OZONE_FS_LISTING_PAGE_SIZE_DEFAULT;

  private boolean hsyncEnabled = OZONE_FS_HSYNC_ENABLED_DEFAULT;
  private boolean isRatisStreamingEnabled
      = OzoneConfigKeys.OZONE_FS_DATASTREAM_ENABLED_DEFAULT;
  private int streamingAutoThreshold;

  private static final String URI_EXCEPTION_TEXT =
      "URL should be one of the following formats: " +
      "ofs://om-service-id/path/to/key  OR " +
      "ofs://om-host.example.com/path/to/key  OR " +
      "ofs://om-host.example.com:5678/path/to/key";

  private static final int PATH_DEPTH_TO_BUCKET = 2;
  private OzoneConfiguration ozoneConfiguration;

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    listingPageSize = conf.getInt(
        OZONE_FS_LISTING_PAGE_SIZE,
        OZONE_FS_LISTING_PAGE_SIZE_DEFAULT);
    listingPageSize = OzoneClientUtils.limitValue(listingPageSize,
        OZONE_FS_LISTING_PAGE_SIZE,
        OZONE_FS_MAX_LISTING_PAGE_SIZE);
    setConf(conf);
    Objects.requireNonNull(name.getScheme(),
        () -> "No scheme provided in " + name);
    Preconditions.checkArgument(getScheme().equals(name.getScheme()),
        "Invalid scheme provided in %s", name);

    String authority = name.getAuthority();
    if (authority == null) {
      // authority is null when fs.defaultFS is not a qualified ofs URI and
      // ofs:/// is passed to the client. matcher will NPE if authority is null
      throw new IllegalArgumentException(URI_EXCEPTION_TEXT);
    }

    String omHostOrServiceId;
    int omPort = -1;
    // Parse hostname and port
    String[] parts = authority.split(":");
    if (parts.length > 2) {
      throw new IllegalArgumentException(URI_EXCEPTION_TEXT);
    }
    omHostOrServiceId = parts[0];
    if (parts.length == 2) {
      try {
        omPort = Integer.parseInt(parts[1]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(URI_EXCEPTION_TEXT);
      }
    }

    try {
      uri = new URIBuilder().setScheme(OZONE_OFS_URI_SCHEME)
          .setHost(authority)
          .build();
      LOG.trace("Ozone URI for OFS initialization is " + uri);

      ConfigurationSource source = getConfSource();
      this.hsyncEnabled = OzoneFSUtils.canEnableHsync(source, true);
      LOG.debug("hsyncEnabled = {}", hsyncEnabled);
      this.adapter = createAdapter(source, omHostOrServiceId, omPort);
      this.adapterImpl = (BasicRootedOzoneClientAdapterImpl) this.adapter;

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
    ozoneConfiguration = OzoneConfiguration.of(getConfSource());
    isRatisStreamingEnabled = ozoneConfiguration.getBoolean(
        OzoneConfigKeys.OZONE_FS_DATASTREAM_ENABLED,
        OzoneConfigKeys.OZONE_FS_DATASTREAM_ENABLED_DEFAULT);
    streamingAutoThreshold = (int) ozoneConfiguration.getStorageSize(
        OzoneConfigKeys.OZONE_FS_DATASTREAM_AUTO_THRESHOLD,
        OzoneConfigKeys.OZONE_FS_DATASTREAM_AUTO_THRESHOLD_DEFAULT,
        StorageUnit.BYTES);
  }

  protected OzoneClientAdapter createAdapter(ConfigurationSource conf,
      String omHost, int omPort) throws IOException {
    return new BasicRootedOzoneClientAdapterImpl(omHost, omPort, conf);
  }

  protected boolean isHsyncEnabled() {
    return hsyncEnabled;
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
    return OZONE_OFS_URI_SCHEME;
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    incrementCounter(Statistic.INVOCATION_OPEN, 1);
    statistics.incrementReadOps(1);
    LOG.trace("open() path: {}", path);
    final String key = pathToKey(path);
    return TracingUtil.executeInNewSpan("ofs open",
        () -> {
          Span span = TracingUtil.getActiveSpan();
          span.setAttribute("path", key);
          return new FSDataInputStream(createFSInputStream(adapter.readFile(key)));
        });
  }

  protected InputStream createFSInputStream(InputStream inputStream) {
    return new OzoneFSInputStream(inputStream, statistics);
  }

  protected void incrementCounter(Statistic statistic) {
    incrementCounter(statistic, 1);
  }

  protected void incrementCounter(Statistic statistic, long count) {
    //don't do anything in this default implementation.
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize,
      short replication, long blockSize,
      Progressable progress) throws IOException {
    LOG.trace("create() path:{}", f);
    incrementCounter(Statistic.INVOCATION_CREATE, 1);
    statistics.incrementWriteOps(1);
    final String key = pathToKey(f);
    return TracingUtil.executeInNewSpan("ofs create",
        () -> createOutputStream(key, replication, overwrite, true));
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path path,
      FsPermission permission,
      EnumSet<CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    incrementCounter(Statistic.INVOCATION_CREATE_NON_RECURSIVE, 1);
    statistics.incrementWriteOps(1);
    final String key = pathToKey(path);
    return TracingUtil.executeInNewSpan("ofs createNonRecursive",
        () ->
            createOutputStream(key,
                replication, flags.contains(CreateFlag.OVERWRITE), false));
  }

  private OutputStream selectOutputStream(String key, short replication,
      boolean overwrite, boolean recursive, int byteWritten)
      throws IOException {
    return isRatisStreamingEnabled && byteWritten > streamingAutoThreshold ?
        createFSDataStreamOutput(adapter.createStreamFile(key, replication, overwrite, recursive))
        : createFSOutputStream(adapter.createFile(
        key, replication, overwrite, recursive));
  }

  private FSDataOutputStream createOutputStream(String key, short replication,
      boolean overwrite, boolean recursive) throws IOException {
    if (isRatisStreamingEnabled) {
      // select OutputStream type based on byteWritten
      final CheckedFunction<Integer, OutputStream, IOException> selector
          = byteWritten -> selectOutputStream(
          key, replication, overwrite, recursive, byteWritten);
      return new FSDataOutputStream(new SelectorOutputStream<>(
          streamingAutoThreshold, selector), statistics);
    }
    return new FSDataOutputStream(createFSOutputStream(
            adapter.createFile(key,
        replication, overwrite, recursive)), statistics);
  }

  protected OzoneFSOutputStream createFSOutputStream(
      OzoneFSOutputStream outputStream) {
    return outputStream;
  }

  protected OzoneFSDataStreamOutput createFSDataStreamOutput(
      OzoneFSDataStreamOutput outputDataStream) {
    return outputDataStream;
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new UnsupportedOperationException("append() Not implemented by the "
        + getClass().getSimpleName() + " FileSystem implementation");
  }

  private class RenameIterator extends OzoneListingIterator {
    private final String srcPath;
    private final String dstPath;
    private final OzoneBucket bucket;
    private final BasicRootedOzoneClientAdapterImpl adapterImpl;

    RenameIterator(Path srcPath, Path dstPath)
        throws IOException {
      super(srcPath);
      this.srcPath = pathToKey(srcPath);
      this.dstPath = pathToKey(dstPath);
      LOG.trace("rename from:{} to:{}", this.srcPath, this.dstPath);
      // Initialize bucket here to reduce number of RPC calls
      OFSPath ofsPath = new OFSPath(srcPath,
          ozoneConfiguration);
      // TODO: Refactor later.
      adapterImpl = (BasicRootedOzoneClientAdapterImpl) adapter;
      this.bucket = adapterImpl.getBucket(ofsPath, false);
    }

    @Override
    boolean processKeyPath(List<String> keyPathList) throws IOException {
      for (String keyPath : keyPathList) {
        String newPath = dstPath.concat(keyPath.substring(srcPath.length()));
        try {
          adapterImpl.rename(this.bucket, keyPath, newPath);
        } catch (OMException ome) {
          LOG.error("Key rename failed for source key: {} to " +
              "destination key: {}.", keyPath, newPath, ome);
          if (OMException.ResultCodes.KEY_ALREADY_EXISTS == ome.getResult() ||
              OMException.ResultCodes.KEY_RENAME_ERROR  == ome.getResult() ||
              OMException.ResultCodes.KEY_NOT_FOUND == ome.getResult()) {
            return false;
          } else {
            throw ome;
          }
        }
      }
      return true;
    }
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
    return TracingUtil.executeInNewSpan("ofs rename",
        () -> renameInSpan(src, dst));
  }

  private boolean renameInSpan(Path src, Path dst) throws IOException {
    Span span = TracingUtil.getActiveSpan();
    span.setAttribute("src", src.toString())
        .setAttribute("dst", dst.toString());
    incrementCounter(Statistic.INVOCATION_RENAME, 1);
    statistics.incrementWriteOps(1);
    if (src.equals(dst)) {
      return true;
    }

    LOG.trace("rename() from: {} to: {}", src, dst);
    if (src.isRoot()) {
      // Cannot rename root of file system
      LOG.trace("Cannot rename the root of a filesystem");
      return false;
    }

    // src and dst should be in the same bucket
    OFSPath ofsSrc = new OFSPath(src,
        ozoneConfiguration);
    OFSPath ofsDst = new OFSPath(dst,
        ozoneConfiguration);
    if (!ofsSrc.isInSameBucketAs(ofsDst)) {
      throw new IOException("Cannot rename a key to a different bucket");
    }
    OzoneBucket bucket = adapterImpl.getBucket(ofsSrc, false);
    if (bucket.getBucketLayout().isFileSystemOptimized()) {
      return renameFSO(bucket, ofsSrc, ofsDst);
    }

    // Cannot rename a directory to its own subdirectory
    Path dstParent = dst.getParent();
    while (dstParent != null && !src.equals(dstParent)) {
      dstParent = dstParent.getParent();
    }
    Preconditions.checkArgument(dstParent == null,
        "Cannot rename a directory to its own subdirectory");
    // Check if the source exists
    FileStatus srcStatus;
    try {
      srcStatus = getFileStatus(src);
    } catch (FileNotFoundException fnfe) {
      // source doesn't exist, return
      return false;
    }

    // Check if the destination exists
    FileStatus dstStatus;
    try {
      dstStatus = getFileStatus(dst);
    } catch (FileNotFoundException fnde) {
      dstStatus = null;
    }

    if (dstStatus == null) {
      // If dst doesn't exist, check whether dst parent dir exists or not
      // if the parent exists, the source can still be renamed to dst path
      dstStatus = getFileStatus(dst.getParent());
      if (!dstStatus.isDirectory()) {
        throw new IOException(String.format(
            "Failed to rename %s to %s, %s is a file", src, dst,
            dst.getParent()));
      }
    } else {
      // if dst exists and source and destination are same,
      // check both the src and dst are of same type
      if (srcStatus.getPath().equals(dstStatus.getPath())) {
        return !srcStatus.isDirectory();
      } else if (dstStatus.isDirectory()) {
        // If dst is a directory, rename source as subpath of it.
        // for example rename /source to /dst will lead to /dst/source
        dst = new Path(dst, src.getName());
        FileStatus[] statuses;
        try {
          statuses = listStatus(dst);
        } catch (FileNotFoundException fnde) {
          statuses = null;
        }

        if (statuses != null && statuses.length > 0) {
          // If dst exists and not a directory not empty
          LOG.warn("Failed to rename {} to {}, file already exists" +
              " or not empty!", src, dst);
          return false;
        }
      } else {
        // If dst is not a directory
        LOG.warn("Failed to rename {} to {}, file already exists!", src, dst);
        return false;
      }
    }

    if (srcStatus.isDirectory()) {
      if (dst.toString().startsWith(src.toString() + OZONE_URI_DELIMITER)) {
        LOG.trace("Cannot rename a directory to a subdirectory of self");
        return false;
      }
    }
    RenameIterator iterator = new RenameIterator(src, dst);
    boolean result = iterator.iterate();
    if (result) {
      createFakeParentDirectory(src);
    }
    return result;
  }

  private boolean renameFSO(OzoneBucket bucket,
      OFSPath srcPath, OFSPath dstPath) throws IOException {
    // construct src and dst key paths
    String srcKeyPath = srcPath.getNonKeyPathNoPrefixDelim() +
        OZONE_URI_DELIMITER + srcPath.getKeyName();
    String dstKeyPath = dstPath.getNonKeyPathNoPrefixDelim() +
        OZONE_URI_DELIMITER + dstPath.getKeyName();
    try {
      adapterImpl.rename(bucket, srcKeyPath, dstKeyPath);
    } catch (OMException ome) {
      LOG.error("rename key failed: {}. source:{}, destin:{}",
          ome.getMessage(), srcKeyPath, dstKeyPath);
      if (OMException.ResultCodes.KEY_ALREADY_EXISTS == ome.getResult() ||
          OMException.ResultCodes.KEY_RENAME_ERROR  == ome.getResult() ||
          OMException.ResultCodes.KEY_NOT_FOUND == ome.getResult()) {
        return false;
      } else {
        throw ome;
      }
    }
    return true;
  }

  /**
   * Intercept rename to trash calls from TrashPolicyDefault.
   */
  @Deprecated
  @Override
  protected void rename(final Path src, final Path dst,
      final Options.Rename... options) throws IOException {
    boolean hasMoveToTrash = false;
    if (options != null) {
      for (Options.Rename option : options) {
        if (option == Options.Rename.TO_TRASH) {
          hasMoveToTrash = true;
          break;
        }
      }
    }
    if (!hasMoveToTrash) {
      // if doesn't have TO_TRASH option, just pass the call to super
      super.rename(src, dst, options);
    } else {
      rename(src, dst);
    }
  }

  @Override
  public Path createSnapshot(Path path, String snapshotName)
          throws IOException {
    String snapshot = TracingUtil.executeInNewSpan("ofs createSnapshot",
        () -> getAdapter().createSnapshot(pathToKey(path), snapshotName));
    return new Path(OzoneFSUtils.trimPathToDepth(path, PATH_DEPTH_TO_BUCKET),
        OM_SNAPSHOT_INDICATOR + OZONE_URI_DELIMITER + snapshot);
  }

  @Override
  public void renameSnapshot(Path path, String snapshotOldName, String snapshotNewName)
      throws IOException {
    getAdapter().renameSnapshot(pathToKey(path), snapshotOldName, snapshotNewName);
  }

  @Override
  public void deleteSnapshot(Path path, String snapshotName)
      throws IOException {
    TracingUtil.executeInNewSpan("ofs deleteSnapshot",
        () -> adapter.deleteSnapshot(pathToKey(path), snapshotName));
  }

  private class DeleteIterator extends OzoneListingIterator {
    private final boolean recursive;
    private final OzoneBucket bucket;
    private final BasicRootedOzoneClientAdapterImpl adapterImpl;

    DeleteIterator(Path f, boolean recursive)
        throws IOException {
      super(f);
      this.recursive = recursive;
      if (getStatus().isDir()
          && !this.recursive
          && listStatus(f).length != 0) {
        throw new PathIsNotEmptyDirectoryException(f.toString());
      }
      // Initialize bucket here to reduce number of RPC calls
      OFSPath ofsPath = new OFSPath(f,
          ozoneConfiguration);
      // TODO: Refactor later.
      adapterImpl = (BasicRootedOzoneClientAdapterImpl) adapter;
      this.bucket = adapterImpl.getBucket(ofsPath, false);
    }

    @Override
    boolean processKeyPath(List<String> keyPathList) {
      LOG.trace("Deleting keys: {}", keyPathList);
      boolean succeed = adapterImpl.deleteObjects(this.bucket, keyPathList);
      // if recursive delete is requested ignore the return value of
      // deleteObject and issue deletes for other keys.
      return recursive || succeed;
    }
  }

  /**
   * To be used only by recursiveBucketDelete().
   */
  private class DeleteIteratorWithFSO extends OzoneListingIterator {
    private final OzoneBucket bucket;
    private final BasicRootedOzoneClientAdapterImpl adapterImpl;
    private boolean recursive;
    private Path f;

    DeleteIteratorWithFSO(Path f, boolean recursive)
        throws IOException {
      super(f, true);
      this.f = f;
      this.recursive = recursive;
      // Initialize bucket here to reduce number of RPC calls
      OFSPath ofsPath = new OFSPath(f,
          ozoneConfiguration);
      adapterImpl = (BasicRootedOzoneClientAdapterImpl) adapter;
      this.bucket = adapterImpl.getBucket(ofsPath, false);
      LOG.debug("Deleting bucket with name {} is via DeleteIteratorWithFSO.",
          bucket.getName());
    }

    @Override
    boolean processKeyPath(List<String> keyPathList) throws IOException {
      LOG.trace("Deleting keys: {}", keyPathList);
      boolean succeed = keyPathList.isEmpty();
      if (recursive && !succeed) {
        succeed = adapterImpl.deleteObjects(keyPathList);
      } else {
        // Non empty paths cannot be deleted when recursive flag is false
        if (!keyPathList.isEmpty()) {
          throw new PathIsNotEmptyDirectoryException(f.toString());
        }
      }
      return succeed;
    }
  }

  private class DeleteIteratorFactory {
    private Path path;
    private boolean recursive;
    private OFSPath ofsPath;

    DeleteIteratorFactory(Path f, boolean recursive) {
      this.path = f;
      this.recursive = recursive;
      this.ofsPath = new OFSPath(f,
          ozoneConfiguration);
    }

    OzoneListingIterator getDeleteIterator()
        throws IOException {
      OzoneListingIterator deleteIterator;
      if (ofsPath.isBucket() &&
          isFSObucket(ofsPath.getVolumeName(), ofsPath.getBucketName())) {
        deleteIterator = new DeleteIteratorWithFSO(path, recursive);
      } else {
        deleteIterator = new DeleteIterator(path, recursive);
      }
      return deleteIterator;
    }
  }

  /**
   * Deletes the children of the input dir path by iterating though the
   * DeleteIterator.
   *
   * @param f directory path to be deleted
   * @return true if successfully deletes all required keys, false otherwise
   * @throws IOException
   */
  private boolean innerDelete(Path f, boolean recursive) throws IOException {
    LOG.trace("delete() path:{} recursive:{}", f, recursive);
    try {
      OzoneListingIterator iterator =
          new DeleteIteratorFactory(f, recursive).getDeleteIterator();
      return iterator.iterate();
    } catch (FileNotFoundException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Couldn't delete {} - does not exist", f);
      }
      return false;
    }
  }


  /**
   * {@inheritDoc}
   *
   * OFS supports volume and bucket deletion, recursive or non-recursive.
   * e.g. delete(new Path("/volume1"), true)
   * But root deletion is explicitly disallowed for safety concerns.
   */
  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return TracingUtil.executeInNewSpan("ofs delete",
        () -> deleteInSpan(f, recursive));
  }

  private boolean deleteInSpan(Path f, boolean recursive) throws IOException {
    incrementCounter(Statistic.INVOCATION_DELETE, 1);
    statistics.incrementWriteOps(1);
    LOG.debug("Delete path {} - recursive {}", f, recursive);

    String key = pathToKey(f);
    OFSPath ofsPath = new OFSPath(key,
        ozoneConfiguration);
    // Handle rm root
    if (ofsPath.isRoot()) {
      // Intentionally drop support for rm root
      // because it is too dangerous and doesn't provide much value
      LOG.warn("delete: OFS does not support rm root. "
          + "To wipe the cluster, please re-init OM instead.");
      return false;
    }

    // Handle delete volume
    if (ofsPath.isVolume()) {
      if (recursive) {
        LOG.warn("Recursive volume delete using ofs is not supported");
        throw new IOException("Recursive volume delete using " +
            "ofs is not supported. " +
            "Instead use 'ozone sh volume delete -r " +
            "o3://<OM_SERVICE_ID>/<Volume_URI>' command");
      }
      return deleteVolume(f, ofsPath);
    }

    // delete bucket
    if (ofsPath.isBucket()) {
      return deleteBucket(f, recursive, ofsPath);
    }
    
    // delete files and directory
    FileStatus status;
    try {
      status = getFileStatus(f);
    } catch (FileNotFoundException ex) {
      LOG.warn("delete: Path does not exist: {}", f);
      return false;
    }

    boolean result;
    if (status.isDirectory()) {
      LOG.debug("delete: Path is a directory: {}", f);

      OzoneBucket bucket = adapterImpl.getBucket(ofsPath, false);
      if (bucket.getBucketLayout().isFileSystemOptimized()) {
        String ofsKeyPath = ofsPath.getNonKeyPathNoPrefixDelim() +
            OZONE_URI_DELIMITER + ofsPath.getKeyName();
        return adapterImpl.deleteObject(ofsKeyPath, recursive);
      }

      // delete inner content of directory with manual recursion
      result = innerDelete(f, recursive);
    } else {
      LOG.debug("delete: Path is a file: {}", f);
      result = adapter.deleteObject(key);
    }

    if (result) {
      // If this delete operation removes all files/directories from the
      // parent directory, then an empty parent directory must be created.
      createFakeParentDirectory(f);
    }

    return result;
  }

  private boolean deleteBucket(Path f, boolean recursive, OFSPath ofsPath)
      throws IOException {
    OzoneBucket bucket;
    try {
      bucket = adapterImpl.getBucket(ofsPath, false);
    } catch (OMException ex) {
      if (ex.getResult() != BUCKET_NOT_FOUND && ex.getResult() != VOLUME_NOT_FOUND) {
        LOG.error("OMException while getting bucket information, considered it as false", ex);
      }
      return false;
    } catch (Exception ex) {
      LOG.error("Exception while getting bucket information, considered it as false", ex);
      return false;
    }
    // check status of normal bucket
    try {
      getFileStatus(f);
    } catch (FileNotFoundException ex) {
      // remove orphan link bucket directly
      if (bucket.isLink()) {
        deleteBucketFromVolume(f, bucket);
        return true;
      }
      LOG.warn("delete: Path does not exist: {}", f);
      return false;
    }

    // handling posix symlink delete behaviours
    // i.) rm [-r] <symlink path>, delete symlink not target bucket contents
    // ii.) rm -r <symlink path>/, delete target bucket contents not symlink
    boolean handleTrailingSlash = f.toString().endsWith(OZONE_URI_DELIMITER);
    // remove link bucket directly if link and
    // rm path does not have trailing slash
    if (bucket.isLink() && !handleTrailingSlash) {
      deleteBucketFromVolume(f, bucket);
      return true;
    }

    // delete inner content of bucket
    boolean result = innerDelete(f, recursive);

    // check if rm path does not have trailing slash
    // if so, the contents of bucket were deleted and skip delete bucket
    // otherwise, Handle delete bucket
    if (!handleTrailingSlash) {
      deleteBucketFromVolume(f, bucket);
    }
    return result;
  }

  private void deleteBucketFromVolume(Path f, OzoneBucket bucket)
      throws IOException {
    OzoneVolume volume =
        adapterImpl.getObjectStore().getVolume(bucket.getVolumeName());
    try {
      volume.deleteBucket(bucket.getName());
    } catch (OMException ex) {
      // bucket is not empty
      if (ex.getResult() == BUCKET_NOT_EMPTY) {
        throw new PathIsNotEmptyDirectoryException(f.toString());
      } else {
        throw ex;
      }
    }
  }

  private boolean deleteVolume(Path f, OFSPath ofsPath)
      throws IOException {
    // verify volume exist
    try {
      getFileStatus(f);
    } catch (FileNotFoundException ex) {
      LOG.warn("delete: Path does not exist: {}", f);
      return false;
    }
    
    String volumeName = ofsPath.getVolumeName();
    try {
      adapterImpl.getObjectStore().deleteVolume(volumeName);
      return true;
    } catch (OMException ex) {
      // volume is not empty
      if (ex.getResult() == VOLUME_NOT_EMPTY) {
        throw new PathIsNotEmptyDirectoryException(f.toString());
      } else {
        throw ex;
      }
    }
  }

  private boolean isFSObucket(String volumeName, String bucketName)
      throws IOException {
    OzoneVolume volume =
        adapterImpl.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    return bucket.getBucketLayout().isFileSystemOptimized();
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
    return TracingUtil.executeInNewSpan("ofs listStatus",
        () -> convertFileStatusArr(listStatusAdapter(f, true)));
  }

  private FileStatus[] convertFileStatusArr(
          List<FileStatusAdapter> adapterArr) {
    FileStatus[] fileStatuses = new FileStatus[adapterArr.size()];
    int index = 0;
    for (FileStatusAdapter statusAdapter : adapterArr) {
      fileStatuses[index++] = convertFileStatus(statusAdapter);      
    }
    return fileStatuses;
  }

  private List<FileStatusAdapter> listStatusAdapter(Path f, boolean lite) throws IOException {
    incrementCounter(Statistic.INVOCATION_LIST_STATUS, 1);
    statistics.incrementReadOps(1);
    LOG.trace("listStatus() path:{}", f);
    int numEntries = listingPageSize;
    LinkedList<FileStatusAdapter> statuses = new LinkedList<>();
    List<FileStatusAdapter> tmpStatusList;
    String startPath = "";
    int entriesAdded;
    do {
      tmpStatusList =
          adapter.listStatus(pathToKey(f), false, startPath,
              numEntries, uri, workingDir, getUsername(), lite);
      entriesAdded = 0;
      if (!tmpStatusList.isEmpty()) {
        if (startPath.isEmpty() || !statuses.getLast().getPath().toString()
            .equals(tmpStatusList.get(0).getPath().toString())) {
          statuses.addAll(tmpStatusList);
          entriesAdded += tmpStatusList.size();
        } else {
          statuses.addAll(tmpStatusList.subList(1, tmpStatusList.size()));
          entriesAdded += tmpStatusList.size() - 1;
        }
        startPath = pathToKey(statuses.getLast().getPath());
      }
      // listStatus returns entries numEntries in size if available.
      // Any lesser number of entries indicate that the required entries have
      // exhausted.
    } while (entriesAdded > 0);

    return statuses;
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
  public Path getHomeDirectory() {
    return makeQualified(new Path(OZONE_USER_DIR + "/"
        + this.userName));
  }

  @Override
  public Token<?> getDelegationToken(String renewer) throws IOException {
    return TracingUtil.executeInNewSpan("ofs getDelegationToken",
        () -> adapter.getDelegationToken(renewer));
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
   * Get the root directory of Trash for a path in OFS.
   * Returns /<volumename>/<bucketname>/.Trash/<username>
   * Caller appends either Current or checkpoint timestamp for trash destination
   * @param path the trash root of the path to be determined.
   * @return trash root
   */
  @Override
  public Path getTrashRoot(Path path) {
    OFSPath ofsPath = new OFSPath(path,
        ozoneConfiguration);
    return this.makeQualified(ofsPath.getTrashRoot(getUsername()));
  }

  /**
   * Get all the trash roots of OFS for current user or for all the users.
   * @param allUsers return trashRoots of all users if true, used by emptier
   * @return trash roots
   */
  @Override
  public Collection<FileStatus> getTrashRoots(boolean allUsers) {
    // Since get all trash roots for one or more users requires listing all
    // volumes and buckets, we will let adapter impl handle it.
    return adapterImpl.getTrashRoots(allUsers, this);
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
    incrementCounter(Statistic.INVOCATION_MKDIRS);
    LOG.trace("mkdir() path:{} ", f);
    String key = pathToKey(f);
    if (isEmpty(key)) {
      return false;
    }
    return TracingUtil.executeInNewSpan("ofs mkdirs",
        () -> mkdir(f));
  }

  @Override
  public long getDefaultBlockSize() {
    return (long) getConfSource().getStorageSize(
        OZONE_SCM_BLOCK_SIZE, OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {    
    return TracingUtil.executeInNewSpan("ofs getFileStatus",
        () -> convertFileStatus(getFileStatusAdapter(f)));
  }

  public FileStatusAdapter getFileStatusAdapter(Path f) throws IOException {
    incrementCounter(Statistic.INVOCATION_GET_FILE_STATUS, 1);
    statistics.incrementReadOps(1);
    LOG.trace("getFileStatus() path:{}", f);
    Path qualifiedPath = f.makeQualified(uri, workingDir);
    String key = pathToKey(qualifiedPath);
    // Handle DistCp /NONE path
    if (key.equals("NONE")) {
      throw new FileNotFoundException("File not found. path /NONE.");
    }
    FileStatusAdapter fileStatus = null;
    try {
      fileStatus = 
        adapter.getFileStatus(key, uri, qualifiedPath, getUsername());
    } catch (IOException e) {
      if (e instanceof OMException) {
        OMException ex = (OMException) e;
        if (ex.getResult().equals(OMException.ResultCodes.KEY_NOT_FOUND) ||
            ex.getResult().equals(OMException.ResultCodes.BUCKET_NOT_FOUND) ||
            ex.getResult().equals(OMException.ResultCodes.VOLUME_NOT_FOUND)) {
          throw new FileNotFoundException("File not found. path:" + f);
        }
      }
      throw e;
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

  @Override
  public OzoneFsServerDefaults getServerDefaults() throws IOException {
    return adapter.getServerDefaults();
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs,
      Path dst) throws IOException {
    incrementCounter(Statistic.INVOCATION_COPY_FROM_LOCAL_FILE);
    super.copyFromLocalFile(delSrc, overwrite, srcs, dst);
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src,
      Path dst) throws IOException {
    incrementCounter(Statistic.INVOCATION_COPY_FROM_LOCAL_FILE);
    super.copyFromLocalFile(delSrc, overwrite, src, dst);
  }

  @Override
  public boolean exists(Path f) throws IOException {
    incrementCounter(Statistic.INVOCATION_EXISTS);
    return super.exists(f);
  }

  @Override
  public FileChecksum getFileChecksum(Path f, long length) throws IOException {
    incrementCounter(Statistic.INVOCATION_GET_FILE_CHECKSUM);
    String key = pathToKey(f);
    return TracingUtil.executeInNewSpan("ofs getFileChecksum",
        () -> adapter.getFileChecksum(key, length));
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    incrementCounter(Statistic.INVOCATION_GLOB_STATUS);
    return super.globStatus(pathPattern);
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter)
      throws IOException {
    incrementCounter(Statistic.INVOCATION_GLOB_STATUS);
    return super.globStatus(pathPattern, filter);
  }

  @Override
  @SuppressWarnings("deprecation")
  public boolean isDirectory(Path f) throws IOException {
    incrementCounter(Statistic.INVOCATION_IS_DIRECTORY);
    return super.isDirectory(f);
  }

  @Override
  @SuppressWarnings("deprecation")
  public boolean isFile(Path f) throws IOException {
    incrementCounter(Statistic.INVOCATION_IS_FILE);
    return super.isFile(f);
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive)
      throws IOException {
    incrementCounter(Statistic.INVOCATION_LIST_FILES);
    return super.listFiles(f, recursive);
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
      throws IOException {
    incrementCounter(Statistic.INVOCATION_LIST_LOCATED_STATUS);
    return new OzoneFileStatusIterator<>(f,
        (stat) -> stat instanceof LocatedFileStatus ? (LocatedFileStatus) stat :
            new LocatedFileStatus(stat, stat.isFile() ? new BlockLocation[0] : null),
        false);
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(Path f)
      throws IOException {
    OFSPath ofsPath = new OFSPath(f,
        ozoneConfiguration);
    if (ofsPath.isRoot() || ofsPath.isVolume()) {
      LOG.warn("Recursive root/volume list using ofs is not supported");
      throw new IOException("Recursive list root/volume " +
          "using ofs is not supported. " +
          "Instead use 'ozone sh key list " +
          "<Volume_URI>' command");
    }
    return new OzoneFileStatusIterator<>(f, stat -> stat, true);
  }

  /**
   * A private class implementation for iterating list of file status.
   *
   * @param <T> the type of the file status.
   */
  private final class OzoneFileStatusIterator<T extends FileStatus>
      implements RemoteIterator<T> {
    private final Function<FileStatus, T> transformFunc;
    private List<FileStatus> thisListing;
    private int i;
    private Path p;
    private T curStat = null;
    private String startPath = "";
    private boolean lite;

    /**
     * Constructor to initialize OzoneFileStatusIterator.
     * Get the first batch of entry for iteration.
     *
     * @param p path to file/directory.
     * @param transformFunc function to convert FileStatus into an expected type.
     * @param lite if true it should look into fetching a lightweight keys from server.
     * @throws IOException
     */
    private OzoneFileStatusIterator(Path p, Function<FileStatus, T> transformFunc, boolean lite) throws IOException {
      this.p = p;
      this.lite = lite;
      this.transformFunc = transformFunc;
      // fetch the first batch of entries in the directory
      thisListing = listFileStatus(p, startPath, lite);
      if (thisListing != null && !thisListing.isEmpty()) {
        startPath = pathToKey(
            thisListing.get(thisListing.size() - 1).getPath());
        LOG.debug("Got {} file status, next start path {}",
            thisListing.size(), startPath);
      }
      i = 0;
    }

    /**
     * @return true if next entry exists false otherwise.
     * @throws IOException
     */
    @Override
    public boolean hasNext() throws IOException {
      while (curStat == null && hasNextNoFilter()) {
        T next;
        FileStatus fileStat = thisListing.get(i++);
        next = transformFunc.apply(fileStat);
        curStat = next;
      }
      return curStat != null;
    }

    /**
     * Checks the next available entry from partial listing if not exhausted
     * or fetches new batch for listing.
     *
     * @return true if next entry exists false otherwise.
     * @throws IOException
     */
    private boolean hasNextNoFilter() throws IOException {
      if (thisListing == null) {
        return false;
      }
      if (i >= thisListing.size()) {
        if (startPath != null && (!thisListing.isEmpty())) {
          // current listing is exhausted & fetch a new listing
          thisListing = listFileStatus(p, startPath, lite);
          if (thisListing != null && !thisListing.isEmpty()) {
            startPath = pathToKey(
                thisListing.get(thisListing.size() - 1).getPath());
            LOG.debug("Got {} file status, next start path {}",
                thisListing.size(), startPath);
          } else {
            return false;
          }
          i = 0;
        }
      }
      return (i < thisListing.size());
    }

    /**
     * @return next entry.
     * @throws IOException
     */
    @Override
    public T next() throws IOException {
      if (hasNext()) {
        T tmp = curStat;
        curStat = null;
        return tmp;
      }
      throw new java.util.NoSuchElementException("No more entry in " + p);
    }
  }

  /**
   * Get all the file status for input path and startPath.
   *
   * @param f
   * @param startPath
   * @param lite if true return lightweight keys
   * @return list of file status.
   * @throws IOException
   */
  private List<FileStatus> listFileStatus(Path f, String startPath, boolean lite)
      throws IOException {
    incrementCounter(Statistic.INVOCATION_LIST_STATUS, 1);
    statistics.incrementReadOps(1);
    LOG.trace("listFileStatus() path:{}", f);
    List<FileStatus> statusList;
    statusList =
        adapter.listStatus(pathToKey(f), false, startPath,
                listingPageSize, uri, workingDir, getUsername(), lite)
            .stream()
            .map(this::convertFileStatus)
            .collect(Collectors.toList());

    if (!statusList.isEmpty() && !startPath.isEmpty()) {
      // Excluding the 1st file status element from list.
      statusList.remove(0);
    }
    return statusList;
  }

  /**
   * Turn a path (relative or otherwise) into an Ozone key.
   *
   * @param path the path of the file.
   * @return the key of the object that represents the file.
   */
  public String pathToKey(Path path) {
    Objects.requireNonNull(path, "Path can't be null!");
    if (!path.isAbsolute()) {
      path = new Path(workingDir, path);
    }
    String key = path.toUri().getPath();
    if (!OzoneFSUtils.isValidName(key)) {
      throw new InvalidPathException("Invalid path Name " + key);
    }
    // removing leading '/' char
    key = key.substring(1);
    LOG.trace("path for key: {} is: {}", key, path);
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
    return "RootedOzoneFileSystem{URI=" + uri + ", "
        + "workingDir=" + workingDir + ", "
        + "userName=" + userName + ", "
        + "statistics=" + statistics
        + "}";
  }

  public ConfigurationSource getConfSource() {
    Configuration conf = super.getConf();
    ConfigurationSource source;
    if (conf instanceof OzoneConfiguration) {
      source = (ConfigurationSource) conf;
    } else {
      source = new LegacyHadoopConfigurationSource(conf);
    }
    return source;
  }

  /**
   * This class provides an interface to iterate through all the keys in the
   * bucket prefixed with the input path key and process them.
   * <p>
   * Each implementing class should define how the keys should be processed
   * through the processKeyPath() function.
   */
  private abstract class OzoneListingIterator {
    private final Path path;
    private final FileStatusAdapter status;
    private String pathKey;
    private Iterator<BasicKeyInfo> keyIterator = null;
    private boolean isFSO;

    OzoneListingIterator(Path path, boolean isFSO)
        throws IOException {
      this.path = path;
      this.status = getFileStatusAdapter(path);
      this.pathKey = pathToKey(path);
      this.isFSO = isFSO;
      if (!isFSO) {
        if (status.isDir()) {
          this.pathKey = addTrailingSlashIfNeeded(pathKey);
        }
        keyIterator = adapter.listKeys(pathKey);
      }
    }

    OzoneListingIterator(Path path) throws IOException {
      // non FSO implementation
      this(path, false);
    }

    /**
     * The output of processKey determines if further iteration through the
     * keys should be done or not.
     *
     * @return true if we should continue iteration of keys, false otherwise.
     * @throws IOException
     */
    abstract boolean processKeyPath(List<String> keyPathList)
        throws IOException;

    /**
     * Iterates through all the keys prefixed with the input path's key and
     * processes the key though processKey().
     * If for any key, the processKey() returns false, then the iteration is
     * stopped and returned with false indicating that all the keys could not
     * be processed successfully.
     *
     * If isFSO is true, call is from DeleteIteratorWithFSO and the list of keys
     * will only contain immediate children i.e top level dirs and files
     *
     * @return true if all keys are processed successfully, false otherwise.
     * @throws IOException
     */
    boolean iterate() throws IOException {
      LOG.trace("Iterating path: {}", path);
      List<String> keyPathList = new ArrayList<>();
      int batchSize = getConf().getInt(OZONE_FS_ITERATE_BATCH_SIZE,
          OZONE_FS_ITERATE_BATCH_SIZE_DEFAULT);
      if (status.isDir()) {
        LOG.trace("Iterating directory: {}", pathKey);
        OFSPath ofsPath = new OFSPath(pathKey,
            ozoneConfiguration);
        String ofsPathPrefix =
            ofsPath.getNonKeyPathNoPrefixDelim() + OZONE_URI_DELIMITER;
        if (isFSO) {
          List<FileStatusAdapter> fileStatuses;
          fileStatuses = listStatusAdapter(path, true);
          for (FileStatusAdapter fileStatus : fileStatuses) {
            String keyName =
                new OFSPath(fileStatus.getPath().toString(),
                    ozoneConfiguration).getKeyName();
            keyPathList.add(ofsPathPrefix + keyName);
          }
          if (keyPathList.size() >= batchSize) {
            if (!processKeyPath(keyPathList)) {
              return false;
            } else {
              keyPathList.clear();
            }
          }
        } else {
          while (keyIterator.hasNext()) {
            BasicKeyInfo key = keyIterator.next();
            // Convert key to full path before passing it to processKeyPath
            // TODO: This conversion is redundant. But want to use only
            //  full path outside AdapterImpl. - Maybe a refactor later.
            String keyPath = ofsPathPrefix + key.getName();
            LOG.trace("iterating key path: {}", keyPath);
            if (!key.getName().equals("")) {
              keyPathList.add(keyPath);
            }
            if (keyPathList.size() >= batchSize) {
              if (!processKeyPath(keyPathList)) {
                return false;
              } else {
                keyPathList.clear();
              }
            }
          }
        }
        if (!keyPathList.isEmpty()) {
          if (!processKeyPath(keyPathList)) {
            return false;
          }
        }
        return true;
      } else {
        LOG.trace("iterating file: {}", path);
        keyPathList.add(pathKey);
        return processKeyPath(keyPathList);
      }
    }

    String getPathKey() {
      return pathKey;
    }

    boolean pathIsDirectory() {
      return status.isDir();
    }

    FileStatusAdapter getStatus() {
      return status;
    }
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

  protected FileStatus constructFileStatus(
          FileStatusAdapter fileStatusAdapter) {
    return new FileStatus(fileStatusAdapter.getLength(),
            fileStatusAdapter.isDir(),
            fileStatusAdapter.getBlockReplication(),
            fileStatusAdapter.getBlocksize(),
            fileStatusAdapter.getModificationTime(),
            fileStatusAdapter.getAccessTime(),
            new FsPermission(fileStatusAdapter.getPermission()),
            fileStatusAdapter.getOwner(),
            fileStatusAdapter.getGroup(),
            fileStatusAdapter.getSymlink(),
            fileStatusAdapter.getPath(),
            false,
            fileStatusAdapter.isEncrypted(),
            fileStatusAdapter.isErasureCoded()
    );
  }

  FileStatus convertFileStatus(FileStatusAdapter fileStatusAdapter) {
    FileStatus fileStatus = constructFileStatus(fileStatusAdapter);
    BlockLocation[] blockLocations = fileStatusAdapter.getBlockLocations();
    if (blockLocations.length == 0) {
      return fileStatus;
    }
    return new LocatedFileStatus(fileStatus, blockLocations);
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    return TracingUtil.executeInNewSpan("ofs getContentSummary",
        () -> getContentSummaryInSpan(f));
  }

  private ContentSummary getContentSummaryInSpan(Path f) throws IOException {
    FileStatusAdapter status = getFileStatusAdapter(f);

    if (status.isFile()) {
      // f is a file
      long length = status.getLength();
      long spaceConsumed = status.getDiskConsumed();

      return new ContentSummary.Builder().length(length).
          fileCount(1).directoryCount(0).spaceConsumed(spaceConsumed).build();
    }
    // f is a directory
    long[] summary = {0, 0, 0, 1};
    int i = 0;
    for (FileStatusAdapter s : listStatusAdapter(f, true)) {
      long length = s.getLength();
      long spaceConsumed = s.getDiskConsumed();
      ContentSummary c = s.isDir() ? getContentSummary(s.getPath()) :
          new ContentSummary.Builder().length(length).
          fileCount(1).directoryCount(0).spaceConsumed(spaceConsumed).build();

      summary[0] += c.getLength();
      summary[1] += c.getSpaceConsumed();
      summary[2] += c.getFileCount();
      summary[3] += c.getDirectoryCount();
    }

    return new ContentSummary.Builder().length(summary[0]).
        fileCount(summary[2]).directoryCount(summary[3]).
        spaceConsumed(summary[1]).build();
  }

  @Override
  public boolean supportsSymlinks() {
    return true;
  }

  @Override
  public Path getLinkTarget(Path f) throws IOException {
    OFSPath ofsPath = new OFSPath(f,
        ozoneConfiguration);
    if (ofsPath.isBucket()) {  // only support bucket links
      OzoneBucket bucket = adapterImpl.getBucket(ofsPath, false);
      if (bucket.isLink()) {
        return new Path(OZONE_URI_DELIMITER +
            bucket.getSourceVolume() + OZONE_URI_DELIMITER +
            bucket.getSourceBucket());
      }
    }
    return f;
  }
  
  public SnapshotDiffReport getSnapshotDiffReport(final Path snapshotDir,
      final String fromSnapshot, final String toSnapshot)
      throws IOException, InterruptedException {
    OFSPath ofsPath =
        new OFSPath(snapshotDir, ozoneConfiguration);
    Preconditions.checkArgument(ofsPath.isBucket(),
        "Unsupported : Path is not a bucket");
    // TODO:HDDS-7681 support snapdiff when toSnapshot="." referring to
    //  current state of the bucket, This can be achieved by calling
    //  createSnapshot and then doing the diff.
    return adapter.getSnapshotDiffReport(snapshotDir, fromSnapshot, toSnapshot);
  }

  @Override
  public void setTimes(Path f, long mtime, long atime) throws IOException {
    incrementCounter(Statistic.INVOCATION_SET_TIMES, 1);
    statistics.incrementWriteOps(1);
    LOG.trace("setTimes() path:{}", f);
    Path qualifiedPath = makeQualified(f);
    String key = pathToKey(qualifiedPath);
    // Handle DistCp /NONE path
    if (key.equals("NONE")) {
      throw new FileNotFoundException("File not found. path /NONE.");
    }
    TracingUtil.executeInNewSpan("ofs setTimes",
        () -> adapter.setTimes(key, mtime, atime));
  }

  protected boolean setSafeModeUtil(SafeModeAction action,
      boolean isChecked)
      throws IOException {
    if (action == SafeModeAction.GET) {
      statistics.incrementReadOps(1);
    } else {
      statistics.incrementWriteOps(1);
    }
    LOG.trace("setSafeMode() action:{}", action);
    return TracingUtil.executeInNewSpan("ofs setSafeMode",
        () -> getAdapter().setSafeMode(action, isChecked));
  }
}
