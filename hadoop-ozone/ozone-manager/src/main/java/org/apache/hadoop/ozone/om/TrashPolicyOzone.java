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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TrashPolicyDefault;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.conf.OMClientConfig;
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
  private static final int MSECS_PER_MINUTE = 60*1000;

  private long emptierInterval;

  private Configuration configuration;

  private OzoneManager om;

  public TrashPolicyOzone(){
  }

  @Override
  public void initialize(Configuration conf, FileSystem fs) {
    this.fs = fs;
    this.configuration = conf;
    this.deletionInterval = (long)(conf.getFloat(
        FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT)
        * MSECS_PER_MINUTE);
    this.emptierInterval = (long)(conf.getFloat(
        FS_TRASH_CHECKPOINT_INTERVAL_KEY, FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT)
        * MSECS_PER_MINUTE);
    if (deletionInterval < 0) {
      LOG.warn("Invalid value {} for deletion interval,"
          + " deletion interaval can not be negative."
          + "Changing to default value 0", deletionInterval);
      this.deletionInterval = 0;
    }
  }

  TrashPolicyOzone(FileSystem fs, Configuration conf, OzoneManager om){
    initialize(conf, fs);
    this.om = om;
  }

  @Override
  public Runnable getEmptier() throws IOException {
    return new TrashPolicyOzone.Emptier((OzoneConfiguration)configuration,
        emptierInterval);
  }


  protected class Emptier implements Runnable {

    private Configuration conf;
    // same as checkpoint interval
    private long emptierInterval;


    private ThreadPoolExecutor executor;

    Emptier(OzoneConfiguration conf, long emptierInterval) throws IOException {
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
      executor = new ThreadPoolExecutor(trashEmptierCorePoolSize,
          trashEmptierCorePoolSize, 1,
          TimeUnit.SECONDS, new ArrayBlockingQueue<>(1024),
          new ThreadPoolExecutor.CallerRunsPolicy());
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
          if (!om.isLeaderReady()){
            continue;
          }
        } catch (InterruptedException e) {
          break;                                  // exit on interrupt
        }

        try {
          now = Time.now();
          if (now >= end) {
            Collection<FileStatus> trashRoots;
            trashRoots = fs.getTrashRoots(true); // list all trash dirs
            LOG.debug("Trash root Size: " + trashRoots.size());
            for (FileStatus trashRoot : trashRoots) {  // dump each trash
              LOG.info("Trashroot:" + trashRoot.getPath().toString());
              if (!trashRoot.isDirectory()) {
                continue;
              }
              TrashPolicyOzone trash = new TrashPolicyOzone(fs, conf, om);
              Runnable task = ()->{
                try {
                  trash.deleteCheckpoint(trashRoot.getPath(), false);
                  trash.createCheckpoint(trashRoot.getPath(),
                      new Date(Time.now()));
                } catch (Exception e) {
                  LOG.error("Unable to checkpoint:" + trashRoot.getPath(), e);
                }
              };
              executor.submit(task);
            }
          }
        } catch (Exception e) {
          LOG.warn("RuntimeException during Trash.Emptier.run(): ", e);
        }
      }
      try {
        fs.close();
      } catch(IOException e) {
        LOG.warn("Trash cannot close FileSystem: ", e);
      } finally {
        executor.shutdown();
        try {
          executor.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          LOG.error("Error attempting to shutdown");
        }
      }
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
        LOG.info("Created trash checkpoint: " + checkpoint.toUri().getPath());
        break;
      } catch (FileAlreadyExistsException e) {
        if (++attempt > 1000) {
          throw new IOException("Failed to checkpoint trash: " + checkpoint);
        }
        checkpoint = checkpointBase.suffix("-" + attempt);
      }
    }
  }

  private void deleteCheckpoint(Path trashRoot, boolean deleteImmediately)
      throws IOException {
    LOG.info("TrashPolicyOzone#deleteCheckpoint for trashRoot: " + trashRoot);

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
        LOG.warn("Unexpected item in trash: "+dir+". Ignoring.");
        continue;
      }

      if (((now - deletionInterval) > time) || deleteImmediately) {
        if (fs.delete(path, true)) {
          LOG.info("Deleted trash checkpoint: "+dir);
        } else {
          LOG.warn("Couldn't delete checkpoint: " + dir + " Ignoring.");
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
