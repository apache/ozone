/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.security.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;

/**
 * Background Service to empty keys that are moved to Trash.
 */
public class TrashDeletingService extends BackgroundService {

    private static final Logger LOG =
            LoggerFactory.getLogger(TrashDeletingService.class);

    // Use single thread for  now
    private final static int KEY_DELETING_CORE_POOL_SIZE = 1;

    private OzoneManager ozoneManager;

    public void setFsConf(Configuration fsConf) {
        this.fsConf = fsConf;
    }

    private Configuration fsConf;

    public TrashDeletingService(long interval, long serviceTimeout,OzoneManager ozoneManager) {
        super("TrashDeletingService", interval, TimeUnit.MILLISECONDS, KEY_DELETING_CORE_POOL_SIZE, serviceTimeout);
        this.ozoneManager = ozoneManager;
        fsConf = new Configuration();
    }


    @Override
    public BackgroundTaskQueue getTasks() {
        BackgroundTaskQueue queue = new BackgroundTaskQueue();
        String rootPath = String.format("%s://%s/",
                OzoneConsts.OZONE_OFS_URI_SCHEME, ozoneManager.getConfiguration().get(OZONE_OM_ADDRESS_KEY));
        // Configuration object where the rootpath is set to an OFS Uri.
        fsConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
        FileSystem fs = null;
        try {
            fs = SecurityUtil.doAsLoginUser(
                    new PrivilegedExceptionAction<FileSystem>() {
                        @Override
                        public FileSystem run() throws IOException {
                            return FileSystem.get(fsConf);
                        }
                    });
        } catch (IOException e) {
            LOG.error("Cannot instantiate filesystem instance");
        }
        queue.add(new TrashDeletingTask(fs,fsConf));
        return queue;
    }

    /**
     * This task creates an emptier thread that deletes all keys obtained from the trashRoots (fs.getTrashRoots)
     */
    private class TrashDeletingTask implements BackgroundTask {

        FileSystem fs;
        Configuration conf;

        public TrashDeletingTask(FileSystem fs, Configuration conf) {
            this.fs = fs;
            this.conf = conf;
        }

        @Override
        public BackgroundTaskResult call() throws Exception {
            Thread emptier = new Thread(new Trash(fs,conf).getEmptier(),"Trash Emptier");
            emptier.setDaemon(true);
            emptier.start();
            return null;
        }

        @Override
        public int getPriority() {
            return 0;
        }
    }
}
