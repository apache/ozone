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
package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Timer;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs.Builder;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;

/**
 * Benchmark for OM throughput (two categories).
 * - object store
 * - fs
 * <p>
 * Remember to add the following configs to your ozone-site.xml:
 * - ozone.key.preallocation.max.blocks: 0
 * - ozone.block.deleting.service.interval: 7d
 */
@CommandLine.Command(name = "om-throughput-benchmark",
    aliases = "otb",
    description = "Benchmark for om throughput.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class OMThroughputBenchmark extends BaseFreonGenerator
    implements Callable<Void> {

  /**
   * The constant LOG.
   */
  public static final Logger LOG =
      LoggerFactory.getLogger(OMThroughputBenchmark.class);

  @CommandLine.Option(names = {"--benchmark"},
      description = "Which type of benchmark to run, expected one of " +
          "[CREATEKEY, DELETEKEY, GETKEY, RENAMEKEY, LISTKEY," +
          " CREATEFILE, DELETEFILE, OPENFILE, RENAMEFILE, FILESTATUS]",
      required = true)
  private BenchmarkType benchmarkType;

  @Option(names = {"-v", "--volume"},
      description = "Name of the bucket which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "vol1")
  private String volumeName;

  @Option(names = {"-b", "--bucket"},
      description = "Name of the bucket which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "bucket1")
  private String bucketName;

  @Option(names = {"-F", "--factor"},
      description = "Replication factor (ONE, THREE)",
      defaultValue = "THREE"
  )
  private ReplicationFactor factor;

  @Option(names = {"--om-service-id"},
      description = "OM Service ID"
  )
  private String omServiceID;

  @Option(names = {"-u", "--useExisting"},
      description = "do not generate keys, use existing ones"
  )
  private boolean useExisting;

  private OzoneConfiguration configuration;

  private OzoneClient ozoneClient;

  private OzoneManagerProtocol ozoneManagerClient;

  private ApiTask task;


  /**
   * Type of benchmarks.
   */
  public enum BenchmarkType {
    // - Object store
    // -- Key level
    CREATEKEY,
    DELETEKEY,
    RENAMEKEY,
    GETKEY,
    LISTKEY,
    // -- Bucket level
    // -- Volume level

    // - FS
    CREATEFILE,
    DELETEFILE,
    OPENFILE,
    RENAMEFILE,
    FILESTATUS,
  }

  @Override
  public Void call() throws Exception {
    configuration = createOzoneConfiguration();

    init();

    initClients();

    prepareTask();

    runTests(task::runTask);

    closeClient();

    return null;
  }

  private void prepareTask() throws Exception {

    switch (benchmarkType) {
    case CREATEKEY:
      task = new CreateKeyTask();
      break;
    case DELETEKEY:
      task = new DeleteKeyTask();
      break;
    case GETKEY:
      task = new GetKeyTask();
      break;
    case RENAMEKEY:
      task = new RenameKeyTask();
      break;
    case LISTKEY:
      task = new ListKeyTask();
      break;

    case CREATEFILE:
      task = new CreateFileTask();
      break;
    case OPENFILE:
      task = new OpenFileTask();
      break;
    case DELETEFILE:
      task = new DeleteFileTask();
      break;
    case FILESTATUS:
      task = new FileStatusTask();
      break;
    case RENAMEFILE:
      task = new RenameFileTask();
      break;
    default:
      throw new IllegalArgumentException(benchmarkType +
          " is not a valid benchmarkType.");
    }

    task.prepare();

  }

  private void initClients() throws Exception {
    LOG.info("init omClient.");
    ozoneManagerClient = createOmClient(configuration, omServiceID);

    LOG.info("init ozoneClient.");
    ozoneClient = createOzoneClient(omServiceID, configuration);
  }

  private void closeClient() throws IOException {
    if (ozoneManagerClient != null) {
      ozoneManagerClient.close();
    }
  }

  /**
   * Generate new object name string for rename action.
   *
   * @param oldName the old name
   * @return the string
   */
  public String generateNewObjectName(String oldName) {
    return oldName + "-new";
  }

  /**
   * Base class for all benchmark task types.
   */
  private abstract class ApiTask {

    private Timer timer;

    public void prepare() throws Exception {
      setTimer();
    }

    public Timer getTimer() {
      return timer;
    }

    public void setTimer() {
      timer = getMetrics().timer(this.getClass().getSimpleName());
    }

    abstract void runTask(long counter) throws Exception;

  }

  private class CreateKeyTask extends ApiTask {

    @Override
    public void runTask(long counter) throws Exception {
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      OmKeyArgs keyArgs = new Builder()
          .setBucketName(bucketName)
          .setVolumeName(volumeName)
          .setReplicationConfig(new RatisReplicationConfig(factor))
          .setKeyName(generateObjectName(counter))
          .setLocationInfoList(new ArrayList<>())
          .setAcls(OzoneAclUtil.getAclList(ugi.getUserName(),
              ugi.getGroupNames(), ALL, ALL))
          .build();

      getTimer().time(() -> {
        OpenKeySession openKeySession = ozoneManagerClient.openKey(keyArgs);

        ozoneManagerClient.commitKey(keyArgs, openKeySession.getId());
        return null;
      });
    }

    @Override
    public void prepare() throws IOException {
      ensureVolumeAndBucketExist(ozoneClient, volumeName, bucketName);
      setTimer();
    }
  }

  private class DeleteKeyTask extends ApiTask {

    @Override
    public void runTask(long counter) throws Exception {
      OmKeyArgs keyArgs = new OmKeyArgs.Builder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .setKeyName(generateObjectName(counter))
          .setRecursive(false)
          .build();
      getTimer().time(() -> {
        ozoneManagerClient.deleteKey(keyArgs);
        return null;
      });
    }

    @Override
    public void prepare() throws IOException {
      ensureVolumeAndBucketExist(ozoneClient, volumeName, bucketName);
      if (!useExisting) {
        CreateKeyTask createKeyTask = new CreateKeyTask();
        createKeyTask.prepare();
        runTests(createKeyTask::runTask);
        createKeyTask.getTimer().time().close();
        getMetrics().remove(createKeyTask.getClass().getSimpleName());
        reinit();
      }
      setTimer();
    }
  }

  private class GetKeyTask extends DeleteKeyTask {

    @Override
    public void runTask(long counter) throws Exception {
      OmKeyArgs keyArgs = new OmKeyArgs.Builder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .setKeyName(generateObjectName(counter))
          .setRefreshPipeline(true)
          .build();
      getTimer().time(() -> {
        ozoneManagerClient.lookupKey(keyArgs);
        return null;
      });
    }
  }

  private class RenameKeyTask extends DeleteKeyTask {

    @Override
    public void runTask(long counter) throws Exception {
      OmKeyArgs keyArgs = new OmKeyArgs.Builder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .setKeyName(generateObjectName(counter))
          .build();
      getTimer().time(() -> {
        ozoneManagerClient.renameKey(keyArgs,
            generateNewObjectName(generateObjectName(counter)));
        return null;
      });

    }
  }

  private class ListKeyTask extends DeleteKeyTask {

    @Override
    public void runTask(long counter) throws Exception {
      getTimer().time(() -> {
        ozoneManagerClient.listKeys(
            volumeName, bucketName, null, getPrefix(), 10);
        return null;
      });
    }
  }

  private class CreateFileTask extends ApiTask {

    @Override
    void runTask(long counter) throws Exception {
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      OmKeyArgs keyArgs = new Builder()
          .setBucketName(bucketName)
          .setVolumeName(volumeName)
          .setReplicationConfig(new RatisReplicationConfig(factor))
          .setKeyName(generateObjectName(counter))
          .setLocationInfoList(new ArrayList<>())
          .setAcls(OzoneAclUtil.getAclList(ugi.getUserName(),
              ugi.getGroupNames(), ALL, ALL))
          .build();

      getTimer().time(() -> {
        OpenKeySession openKeySession = ozoneManagerClient.createFile(keyArgs,
            true, true);

        ozoneManagerClient.commitKey(keyArgs, openKeySession.getId());

        return null;
      });

    }

    @Override
    public void prepare() throws Exception {
      ensureVolumeAndBucketExist(ozoneClient, volumeName, bucketName);
      setTimer();
    }
  }

  private class DeleteFileTask extends ApiTask {

    @Override
    void runTask(long counter) throws Exception {
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      OmKeyArgs keyArgs = new Builder()
          .setBucketName(bucketName)
          .setVolumeName(volumeName)
          .setReplicationConfig(new RatisReplicationConfig(factor))
          .setKeyName(generateObjectName(counter))
          .setLocationInfoList(new ArrayList<>())
          .setAcls(OzoneAclUtil.getAclList(ugi.getUserName(),
              ugi.getGroupNames(), ALL, ALL))
          .build();

      getTimer().time(() -> {
        ozoneManagerClient.getFileStatus(keyArgs);
        ozoneManagerClient.deleteKey(keyArgs);
        return null;
      });
    }

    @Override
    public void prepare() throws Exception {
      ensureVolumeAndBucketExist(ozoneClient, volumeName, bucketName);
      if (!useExisting) {
        CreateFileTask createFileTask = new CreateFileTask();
        createFileTask.prepare();
        runTests(createFileTask::runTask);
        createFileTask.getTimer().time().close();
        getMetrics().remove(createFileTask.getClass().getSimpleName());
        reinit();
      }
      setTimer();
    }
  }

  private class OpenFileTask extends DeleteFileTask {

    @Override
    void runTask(long counter) throws Exception {
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      OmKeyArgs keyArgs = new Builder()
          .setBucketName(bucketName)
          .setVolumeName(volumeName)
          .setReplicationConfig(new RatisReplicationConfig(factor))
          .setKeyName(generateObjectName(counter))
          .setLocationInfoList(new ArrayList<>())
          .setAcls(OzoneAclUtil.getAclList(ugi.getUserName(),
              ugi.getGroupNames(), ALL, ALL))
          .build();

      getTimer().time(() -> {
        ozoneManagerClient.lookupFile(keyArgs);
        return null;
      });
    }
  }

  private class FileStatusTask extends DeleteFileTask {

    @Override
    void runTask(long counter) throws Exception {
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      OmKeyArgs keyArgs = new Builder()
          .setBucketName(bucketName)
          .setVolumeName(volumeName)
          .setReplicationConfig(new RatisReplicationConfig(factor))
          .setKeyName(generateObjectName(counter))
          .setLocationInfoList(new ArrayList<>())
          .setAcls(OzoneAclUtil.getAclList(ugi.getUserName(),
              ugi.getGroupNames(), ALL, ALL))
          .build();

      getTimer().time(() -> {
        ozoneManagerClient.getFileStatus(keyArgs);
        return null;
      });
    }
  }

  private class RenameFileTask extends DeleteFileTask {

    @Override
    void runTask(long counter) throws Exception {
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      OmKeyArgs keyArgs = new Builder()
          .setBucketName(bucketName)
          .setVolumeName(volumeName)
          .setReplicationConfig(new RatisReplicationConfig(factor))
          .setKeyName(generateObjectName(counter))
          .setLocationInfoList(new ArrayList<>())
          .setAcls(OzoneAclUtil.getAclList(ugi.getUserName(),
              ugi.getGroupNames(), ALL, ALL))
          .build();

      getTimer().time(() -> {
        ozoneManagerClient.renameKey(keyArgs,
            generateNewObjectName(generateObjectName(counter)));
        return null;
      });
    }
  }

}
