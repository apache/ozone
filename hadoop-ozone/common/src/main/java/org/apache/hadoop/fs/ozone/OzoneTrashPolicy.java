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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TrashPolicyDefault;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TrashPolicy for Ozone Specific Trash Operations.
 */
public class OzoneTrashPolicy extends TrashPolicyDefault {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneTrashPolicy.class);

  protected static final Path CURRENT = new Path("Current");

  protected static final int MSECS_PER_MINUTE = 60 * 1000;

  private static final FsPermission PERMISSION =
      new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);
  private OzoneConfiguration ozoneConfiguration;

  public OzoneConfiguration getOzoneConfiguration() {
    return ozoneConfiguration;
  }

  @Override
  public void initialize(Configuration conf, FileSystem fs) {
    this.fs = fs;
    ozoneConfiguration = OzoneConfiguration.of(conf);
    float hadoopTrashInterval = conf.getFloat(
        FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT);
    // check whether user has configured ozone specific trash-interval
    // if not fall back to hadoop configuration
    this.deletionInterval = (long)(conf.getFloat(
        OMConfigKeys.OZONE_FS_TRASH_INTERVAL_KEY, hadoopTrashInterval)
        * MSECS_PER_MINUTE);
  }

  @Override
  public boolean moveToTrash(Path path) throws IOException {
    if (validatePath(path)) {
      if (!isEnabled()) {
        return false;
      }

      if (!path.isAbsolute()) {                  // make path absolute
        path = new Path(fs.getWorkingDirectory(), path);
      }

      // check that path exists
      fs.getFileStatus(path);
      String qpath = fs.makeQualified(path).toString();

      Path trashRoot = fs.getTrashRoot(path);
      Path trashCurrent = new Path(trashRoot, CURRENT);
      if (qpath.startsWith(trashRoot.toString())) {
        return false;                               // already in trash
      }

      if (trashRoot.getParent().toString().startsWith(qpath)) {
        throw new IOException("Cannot move \"" + path
            + "\" to the trash, as it contains the trash");
      }

      Path trashPath;
      Path baseTrashPath;
      if (fs.getUri().getScheme().equals(OzoneConsts.OZONE_OFS_URI_SCHEME)) {
        OFSPath ofsPath = new OFSPath(path, ozoneConfiguration);
        // trimming volume and bucket in order to be compatible with o3fs
        // Also including volume and bucket name in the path is redundant as
        // the key is already in a particular volume and bucket.
        Path trimmedVolumeAndBucket =
            new Path(OzoneConsts.OZONE_URI_DELIMITER
                + ofsPath.getKeyName());
        trashPath = makeTrashRelativePath(trashCurrent, trimmedVolumeAndBucket);
        baseTrashPath = makeTrashRelativePath(trashCurrent,
            trimmedVolumeAndBucket.getParent());
      } else {
        trashPath = makeTrashRelativePath(trashCurrent, path);
        baseTrashPath = makeTrashRelativePath(trashCurrent, path.getParent());
      }

      IOException cause = null;

      // try twice, in case checkpoint between the mkdirs() & rename()
      for (int i = 0; i < 2; i++) {
        try {
          if (!fs.mkdirs(baseTrashPath, PERMISSION)) {      // create current
            LOG.warn("Can't create(mkdir) trash directory: " + baseTrashPath);
            return false;
          }
        } catch (FileAlreadyExistsException e) {
          // find the path which is not a directory, and modify baseTrashPath
          // & trashPath, then mkdirs
          Path existsFilePath = baseTrashPath;
          while (!fs.exists(existsFilePath)) {
            existsFilePath = existsFilePath.getParent();
          }
          baseTrashPath = new Path(baseTrashPath.toString()
              .replace(existsFilePath.toString(),
                  existsFilePath.toString() + Time.now()));
          trashPath = new Path(baseTrashPath, trashPath.getName());
          // retry, ignore current failure
          --i;
          continue;
        } catch (IOException e) {
          LOG.warn("Can't create trash directory: " + baseTrashPath, e);
          cause = e;
          break;
        }
        try {
          // if the target path in Trash already exists, then append with
          // a current time in millisecs.
          String orig = trashPath.toString();

          while (fs.exists(trashPath)) {
            trashPath = new Path(orig + Time.now());
          }

          // move to current trash
          boolean renamed = fs.rename(path, trashPath);
          if (!renamed) {
            LOG.error("Failed to move to trash: {}", path);
            throw new IOException("Failed to move to trash: " + path);
          }
          LOG.info("Moved: '" + path + "' to trash at: " + trashPath);
          return true;
        } catch (IOException e) {
          cause = e;
        }
      }
      throw (IOException) new IOException("Failed to move to trash: " + path)
          .initCause(cause);
    }
    return false;
  }

  private boolean validatePath(Path path) throws IOException {
    String key = path.toUri().getPath();
    // Check to see if bucket is path item to be deleted.
    // Cannot moveToTrash if bucket is deleted,
    // return error for this condition
    OFSPath ofsPath = new OFSPath(key.substring(1), ozoneConfiguration);
    if (path.isRoot() || ofsPath.isBucket()) {
      throw new IOException("Recursive rm of bucket "
          + path + " not permitted");
    }

    Path trashRoot = this.fs.getTrashRoot(path);

    LOG.debug("Key path to moveToTrash: {}", key);
    String trashRootKey = trashRoot.toUri().getPath();
    LOG.debug("TrashrootKey for moveToTrash: {}", trashRootKey);

    if (!OzoneFSUtils.isValidName(key)) {
      throw new InvalidPathException("Invalid path Name " + key);
    }
    // first condition tests when length key is <= length trash
    // and second when length key > length trash
    if ((key.contains(this.fs.TRASH_PREFIX)) && (trashRootKey.startsWith(key))
        || key.startsWith(trashRootKey)) {
      return false;
    }
    return true;
  }

  private Path makeTrashRelativePath(Path basePath, Path rmFilePath) {
    return Path.mergePaths(basePath, rmFilePath);
  }

}
