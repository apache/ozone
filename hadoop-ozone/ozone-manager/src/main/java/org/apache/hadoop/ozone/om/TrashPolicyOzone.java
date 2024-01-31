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
package org.apache.hadoop.ozone.om;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TrashPolicyDefault;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.conf.OMClientConfig;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT;

/**
 * TrashPolicy for Ozone Specific Trash Operations.Through this implementation
 *  of TrashPolicy ozone-specific trash optimizations are/will be made such as
 *  having a multithreaded TrashEmptier.
 */
public class TrashPolicyOzone extends TrashPolicyDefault {

  private static final Logger LOG =
      LoggerFactory.getLogger(TrashPolicyOzone.class);

  private static final Path CURRENT = new Path("Current");

  private static final FsPermission PERMISSION =
      new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);

  private static final DateFormat CHECKPOINT = new SimpleDateFormat(
      "yyMMddHHmmss");
  /** Format of checkpoint directories used prior to Hadoop 0.23. */
  private static final DateFormat OLD_CHECKPOINT =
      new SimpleDateFormat("yyMMddHHmm");
  private static final int MSECS_PER_MINUTE = 60 * 1000;

  private long emptierInterval;

  private Configuration configuration;

  private OzoneManager om;

  private OzoneConfiguration ozoneConfiguration;

  public TrashPolicyOzone() {
  }

  @Override
  public void initialize(Configuration conf, FileSystem fs) {
    this.fs = fs;
    this.configuration = conf;
    float hadoopTrashInterval = conf.getFloat(
        FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT);
    // check whether user has configured ozone specific trash-interval
    // if not fall back to hadoop configuration
    this.deletionInterval = (long)(conf.getFloat(
        OMConfigKeys.OZONE_FS_TRASH_INTERVAL_KEY, hadoopTrashInterval)
        * MSECS_PER_MINUTE);
    float hadoopCheckpointInterval = conf.getFloat(
        FS_TRASH_CHECKPOINT_INTERVAL_KEY,
        FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT);
    // check whether user has configured ozone specific
    // trash- checkpoint-interval
    // if not fall back to hadoop configuration
    this.emptierInterval = (long)(conf.getFloat(
        OMConfigKeys.OZONE_FS_TRASH_CHECKPOINT_INTERVAL_KEY,
        hadoopCheckpointInterval)
        * MSECS_PER_MINUTE);
    if (deletionInterval < 0) {
      LOG.warn("Invalid value {} for deletion interval,"
          + " deletion interaval can not be negative."
          + "Changing to default value 0", deletionInterval);
      this.deletionInterval = 0;
    }
    ozoneConfiguration = OzoneConfiguration.of(this.configuration);
  }

  TrashPolicyOzone(FileSystem fs, Configuration conf, OzoneManager om) {
    initialize(conf, fs);
    this.om = om;
  }

  @Override
  public Runnable getEmptier() throws IOException {
    return new TrashPolicyOzone.Emptier((OzoneConfiguration) configuration,
        emptierInterval, om.getThreadNamePrefix());
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
          + path.toString() + " not permitted");
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

  protected class Emptier implements Runnable {

    private Configuration conf;
    // same as checkpoint interval
    private long emptierInterval;


    private ThreadPoolExecutor executor;

    Emptier(OzoneConfiguration conf, long emptierInterval,
            String threadNamePrefix) throws IOException {
      this.conf = conf;
      this.emptierInterval = emptierInterval;
      if (emptierInterval > deletionInterval || emptierInterval <= 0) {
        LOG.info("The configured checkpoint interval is " +
            (emptierInterval / MSECS_PER_MINUTE) + " minutes." +
            " Using an interval of " +
            (deletionInterval / MSECS_PER_MINUTE) +
            " minutes that is used for deletion instead");
        this.emptierInterval = deletionInterval;
      }
      int trashEmptierCorePoolSize = conf.getObject(OMClientConfig.class)
          .getTrashEmptierPoolSize();
      LOG.info("Ozone Manager trash configuration: Deletion interval = "
          + (deletionInterval / MSECS_PER_MINUTE)
          + " minutes, Emptier interval = "
          + (this.emptierInterval / MSECS_PER_MINUTE) + " minutes.");
      executor = new ThreadPoolExecutor(
          trashEmptierCorePoolSize,
          trashEmptierCorePoolSize,
          1,
          TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(1024),
          new ThreadFactoryBuilder()
              .setNameFormat(threadNamePrefix + "TrashEmptier-%d")
              .build(),
          new ThreadPoolExecutor.CallerRunsPolicy()
      );
    }

    @Override
    public void run() {
      if (emptierInterval == 0) {
        return;                                   // trash disabled
      }
      long now, end;
      while (true) {
        now = Time.now();
        end = ceiling(now, emptierInterval);
        try {
          // sleep for interval
          Thread.sleep(end - now);
          // if not leader, thread will always be sleeping
          if (!om.isLeaderReady()) {
            continue;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;                                  // exit on interrupt
        }

        try {
          om.getMetrics().incNumTrashActiveCycles();
          now = Time.now();
          if (now >= end) {
            Collection<FileStatus> trashRoots;
            trashRoots = fs.getTrashRoots(true); // list all trash dirs
            LOG.debug("Trash root Size: {}", trashRoots.size());
            for (FileStatus trashRoot : trashRoots) {  // dump each trash
              LOG.debug("Trashroot: {}", trashRoot.getPath());
              if (!trashRoot.isDirectory()) {
                continue;
              }
              TrashPolicyOzone trash = new TrashPolicyOzone(fs, conf, om);
              Path trashRootPath = trashRoot.getPath();
              Runnable task = getEmptierTask(trashRootPath, trash, false);
              om.getMetrics().incNumTrashRootsEnqueued();
              executor.submit(task);
            }
          }
        } catch (Exception e) {
          om.getMetrics().incNumTrashFails();
          LOG.warn("RuntimeException during Trash.Emptier.run(): ", e);
        }
      }
      try {
        fs.close();
      } catch (IOException e) {
        LOG.warn("Trash cannot close FileSystem: ", e);
      } finally {
        executor.shutdown();
        try {
          executor.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          LOG.error("Error attempting to shutdown", e);
          Thread.currentThread().interrupt();
        }
      }
    }

    private Runnable getEmptierTask(Path trashRootPath, TrashPolicyOzone trash,
        boolean deleteImmediately) {
      Runnable task = () -> {
        try {
          om.getMetrics().incNumTrashRootsProcessed();
          trash.deleteCheckpoint(trashRootPath, deleteImmediately);
          trash.createCheckpoint(trashRootPath, new Date(Time.now()));
        } catch (Exception e) {
          om.getMetrics().incNumTrashFails();
          LOG.error("Unable to checkpoint:" + trashRootPath, e);
        }
      };
      return task;
    }

    private long ceiling(long time, long interval) {
      return floor(time, interval) + interval;
    }
    private long floor(long time, long interval) {
      return (time / interval) * interval;
    }

  }

  private void createCheckpoint(Path trashRoot, Date date) throws IOException {
    if (!fs.exists(new Path(trashRoot, CURRENT))) {
      return;
    }
    Path checkpointBase;
    synchronized (CHECKPOINT) {
      checkpointBase = new Path(trashRoot, CHECKPOINT.format(date));
    }
    Path checkpoint = checkpointBase;
    Path current = new Path(trashRoot, CURRENT);

    int attempt = 0;
    while (true) {
      try {
        fs.rename(current, checkpoint);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Created trash checkpoint: {}",
                  checkpoint.toUri().getPath());
        }
        break;
      } catch (FileAlreadyExistsException e) {
        if (++attempt > 1000) {
          om.getMetrics().incNumTrashFails();
          throw new IOException("Failed to checkpoint trash: " + checkpoint);
        }
        checkpoint = checkpointBase.suffix("-" + attempt);
      }
    }
  }

  private void deleteCheckpoint(Path trashRoot, boolean deleteImmediately)
      throws IOException {
    LOG.debug("TrashPolicyOzone#deleteCheckpoint for trashRoot: {}",
            trashRoot);

    FileStatus[] dirs = null;
    try {
      dirs = fs.listStatus(trashRoot); // scan trash sub-directories
    } catch (FileNotFoundException fnfe) {
      return;
    }

    long now = Time.now();
    for (int i = 0; i < dirs.length; i++) {
      Path path = dirs[i].getPath();
      String dir = path.toUri().getPath();
      String name = path.getName();
      if (name.equals(CURRENT.getName())) {         // skip current
        continue;
      }

      long time;
      try {
        time = getTimeFromCheckpoint(name);
      } catch (ParseException e) {
        om.getMetrics().incNumTrashFails();
        LOG.warn("Unexpected item in trash: {} . Ignoring.", dir);
        continue;
      }

      if (((now - deletionInterval) > time) || deleteImmediately) {
        if (fs.delete(path, true)) {
          LOG.debug("Deleted trash checkpoint:{} ", dir);
        } else {
          om.getMetrics().incNumTrashFails();
          LOG.warn("Couldn't delete checkpoint: {} Ignoring.", dir);
        }
      }
    }
  }

  private long getTimeFromCheckpoint(String name) throws ParseException {
    long time;

    try {
      synchronized (CHECKPOINT) {
        time = CHECKPOINT.parse(name).getTime();
      }
    } catch (ParseException pe) {
      // Check for old-style checkpoint directories left over
      // after an upgrade from Hadoop 1.x
      synchronized (OLD_CHECKPOINT) {
        time = OLD_CHECKPOINT.parse(name).getTime();
      }
    }

    return time;
  }
}
