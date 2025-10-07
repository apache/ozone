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

package org.apache.hadoop.ozone.freon;

import static org.apache.hadoop.ozone.conf.OzoneServiceConfig.DEFAULT_SHUTDOWN_HOOK_PRIORITY;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageSize;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.util.ShutdownHookManager;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.VersionInfo;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/**
 * Data generator tool to generate as much keys as possible.
 */
@Command(name = "randomkeys",
    aliases = "rk",
    description = "Generate volumes/buckets and put generated keys.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
@SuppressWarnings("java:S2245") // no need for secure random
public final class RandomKeyGenerator implements Callable<Void>, FreonSubcommand {

  @ParentCommand
  private Freon freon;

  private static final String DURATION_FORMAT = "HH:mm:ss,SSS";

  private static final int QUANTILES = 10;

  private static final int CHECK_INTERVAL_MILLIS = 100;

  private byte[] keyValueBuffer = null;

  private static final String DIGEST_ALGORITHM = "MD5";
  // A common initial MesssageDigest for each key without its UUID
  private MessageDigest commonInitialMD = null;

  private static final Logger LOG =
      LoggerFactory.getLogger(RandomKeyGenerator.class);

  private volatile boolean completed = false;
  private volatile Throwable exception;

  @Option(names = {"--num-of-threads", "--numOfThreads"},
      description = "number of threads to be launched for the run. Full name " +
          "--numOfThreads will be removed in later versions.",
      defaultValue = "10")
  private int numOfThreads = 10;

  @Option(names = {"--num-of-volumes", "--numOfVolumes"},
      description = "specifies number of Volumes to be created in offline " +
          "mode. Full name --numOfVolumes will be removed in later versions.",
      defaultValue = "10")
  private int numOfVolumes = 10;

  @Option(names = {"--num-of-buckets", "--numOfBuckets"},
      description = "specifies number of Buckets to be created per Volume. " +
          "Full name --numOfBuckets will be removed in later versions.",
      defaultValue = "1000")
  private int numOfBuckets = 1000;

  @Option(
      names = {"--num-of-keys", "--numOfKeys"},
      description = "specifies number of Keys to be created per Bucket. Full" +
          " name --numOfKeys will be removed in later versions.",
      defaultValue = "500000"
  )
  private int numOfKeys = 500000;

  @Option(
      names = {"--key-size", "--keySize"},
      description = "Specifies the size of Key in bytes to be created. Full" +
          " name --keySize will be removed in later versions. " +
          StorageSizeConverter.STORAGE_SIZE_DESCRIPTION,
      defaultValue = "10KB",
      converter = StorageSizeConverter.class
  )
  private StorageSize keySize;

  @Option(
      names = {"--validate-writes", "--validateWrites"},
      description = "Specifies whether to validate keys after writing. Full" +
          " name --validateWrites will be removed in later versions."
  )
  private boolean validateWrites = false;

  @Option(names = {"--num-of-validate-threads", "--numOfValidateThreads"},
      description = "number of threads to be launched for validating keys." +
          "Full name --numOfValidateThreads will be removed in later versions.",
      defaultValue = "1")
  private int numOfValidateThreads = 1;

  @Option(
      names = {"--buffer-size", "--bufferSize"},
      description = "Specifies the buffer size while writing. Full name " +
          "--bufferSize will be removed in later versions.",
      defaultValue = "4096"
  )
  private int bufferSize = 4096;

  @Option(
      names = "--json",
      description = "directory where json is created."
  )
  private String jsonDir;

  @Mixin
  private FreonReplicationOptions replication;

  @Option(
      names = "--om-service-id",
      description = "OM Service ID"
  )
  private String omServiceID = null;

  @Option(
      names = "--clean-objects",
      description = "Specifies whether to clean the random generated " +
          "volumes, buckets and keys."
  )
  private boolean cleanObjects = false;

  @Option(
      names = "--bucket-layout",
      description = "Specifies the bucket layout (e.g., FILE_SYSTEM_OPTIMIZED, OBJECT_STORE, LEGACY)."
  )
  private BucketLayout bucketLayout;

  private ReplicationConfig replicationConfig;

  @SuppressWarnings("PMD.SingularField")
  private int threadPoolSize;

  private OzoneClient ozoneClient;
  private ObjectStore objectStore;
  @SuppressWarnings("PMD.SingularField")
  private ExecutorService executor;

  private long startTime;
  private long jobStartTime;

  private AtomicLong volumeCreationTime;
  private AtomicLong bucketCreationTime;
  private AtomicLong keyCreationTime;
  private AtomicLong keyWriteTime;

  private AtomicLong totalBytesWritten;

  private int totalBucketCount;
  private long totalKeyCount;
  private AtomicInteger volumeCounter;
  private AtomicInteger bucketCounter;
  private AtomicLong keyCounter;
  private Map<Integer, OzoneVolume> volumes;
  private Map<Integer, OzoneBucket> buckets;

  private AtomicInteger numberOfVolumesCreated;
  private AtomicInteger numberOfBucketsCreated;
  private AtomicLong numberOfKeysAdded;

  private AtomicInteger cleanedBucketCounter;
  private AtomicInteger numberOfBucketsCleaned;
  private AtomicInteger numberOfVolumesCleaned;

  private AtomicLong totalWritesValidated;
  private AtomicLong writeValidationSuccessCount;
  private AtomicLong writeValidationFailureCount;

  private BlockingQueue<KeyValidate> validationQueue;
  private ArrayList<Histogram> histograms = new ArrayList<>();

  private OzoneConfiguration ozoneConfiguration;
  @SuppressWarnings("PMD.SingularField")
  private ProgressBar progressbar;

  public RandomKeyGenerator() {
    // for picocli
  }

  @VisibleForTesting
  RandomKeyGenerator(OzoneConfiguration ozoneConfiguration) {
    this.ozoneConfiguration = ozoneConfiguration;
  }

  public void init(OzoneConfiguration configuration) throws IOException {
    startTime = System.nanoTime();
    jobStartTime = System.currentTimeMillis();
    volumeCreationTime = new AtomicLong();
    bucketCreationTime = new AtomicLong();
    keyCreationTime = new AtomicLong();
    keyWriteTime = new AtomicLong();
    totalBytesWritten = new AtomicLong();
    numberOfVolumesCreated = new AtomicInteger();
    numberOfBucketsCreated = new AtomicInteger();
    numberOfKeysAdded = new AtomicLong();
    volumeCounter = new AtomicInteger();
    bucketCounter = new AtomicInteger();
    keyCounter = new AtomicLong();
    volumes = new ConcurrentHashMap<>();
    buckets = new ConcurrentHashMap<>();
    cleanedBucketCounter = new AtomicInteger();
    numberOfBucketsCleaned = new AtomicInteger();
    numberOfVolumesCleaned = new AtomicInteger();
    if (omServiceID != null) {
      ozoneClient = OzoneClientFactory.getRpcClient(omServiceID, configuration);
    } else {
      ozoneClient = OzoneClientFactory.getRpcClient(configuration);
    }
    objectStore = ozoneClient.getObjectStore();
    for (FreonOps ops : FreonOps.values()) {
      histograms.add(ops.ordinal(), new Histogram(new UniformReservoir()));
    }
    if (freon != null) {
      freon.startHttpServer();
    }
  }

  @Override
  public Void call() throws Exception {
    if (ozoneConfiguration == null) {
      ozoneConfiguration = freon.getOzoneConf();
    }
    if (!ozoneConfiguration.getBoolean(
        HddsConfigKeys.HDDS_CONTAINER_PERSISTDATA,
        HddsConfigKeys.HDDS_CONTAINER_PERSISTDATA_DEFAULT)) {
      LOG.info("Override validateWrites to false, because "
          + HddsConfigKeys.HDDS_CONTAINER_PERSISTDATA + " is set to false.");
      validateWrites = false;
    }
    init(ozoneConfiguration);

    replicationConfig = replication.fromParamsOrConfig(ozoneConfiguration);

    keyValueBuffer = StringUtils.string2Bytes(
        RandomStringUtils.secure().nextAscii(bufferSize));

    // Compute the common initial digest for all keys without their UUID
    if (validateWrites) {
      commonInitialMD = DigestUtils.getDigest(DIGEST_ALGORITHM);
      for (long nrRemaining = keySize.toBytes(); nrRemaining > 0;
          nrRemaining -= bufferSize) {
        int curSize = (int)Math.min(bufferSize, nrRemaining);
        commonInitialMD.update(keyValueBuffer, 0, curSize);
      }
    }

    totalBucketCount = numOfVolumes * numOfBuckets;
    totalKeyCount = totalBucketCount * numOfKeys;

    LOG.info("Number of Threads: {}", numOfThreads);
    threadPoolSize = numOfThreads;
    executor = Executors.newFixedThreadPool(threadPoolSize);
    addShutdownHook();

    LOG.info("Number of Volumes: {}.", numOfVolumes);
    LOG.info("Number of Buckets per Volume: {}.", numOfBuckets);
    LOG.info("Number of Keys per Bucket: {}.", numOfKeys);
    LOG.info("Key size: {} bytes", keySize.toBytes());
    LOG.info("Buffer size: {} bytes", bufferSize);
    LOG.info("validateWrites : {}", validateWrites);
    LOG.info("Number of Validate Threads: {}", numOfValidateThreads);
    LOG.info("cleanObjects : {}", cleanObjects);
    for (int i = 0; i < numOfThreads; i++) {
      executor.execute(new ObjectCreator());
    }

    ExecutorService validateExecutor = null;
    if (validateWrites) {
      totalWritesValidated = new AtomicLong();
      writeValidationSuccessCount = new AtomicLong();
      writeValidationFailureCount = new AtomicLong();

      validationQueue = new LinkedBlockingQueue<>();
      validateExecutor = Executors.newFixedThreadPool(numOfValidateThreads);
      for (int i = 0; i < numOfValidateThreads; i++) {
        validateExecutor.execute(new Validator());
      }
      LOG.info("Data validation is enabled.");
    }

    LongSupplier currentValue = numberOfKeysAdded::get;
    progressbar = new ProgressBar(System.out, totalKeyCount, currentValue);

    LOG.info("Starting progress bar Thread.");

    progressbar.start();

    // wait until all keys are added or exception occurred.
    while ((numberOfKeysAdded.get() != totalKeyCount)
           && exception == null) {
      Thread.sleep(CHECK_INTERVAL_MILLIS);
    }
    executor.shutdown();
    executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    completed = true;

    if (exception != null) {
      progressbar.terminate();
    } else {
      progressbar.shutdown();
    }

    if (validateExecutor != null) {
      while (!validationQueue.isEmpty()) {
        Thread.sleep(CHECK_INTERVAL_MILLIS);
      }
      validateExecutor.shutdown();
      validateExecutor.awaitTermination(Integer.MAX_VALUE,
          TimeUnit.MILLISECONDS);
    }
    if (cleanObjects && exception == null) {
      doCleanObjects();
    }
    ozoneClient.close();
    if (exception != null) {
      throw new RuntimeException(exception);
    }
    return null;
  }

  /**
   * Adds ShutdownHook to print statistics.
   */
  private void addShutdownHook() {
    ShutdownHookManager.get().addShutdownHook(() -> {
      printStats(System.out);
      if (freon != null) {
        freon.stopHttpServer();
      }
    }, DEFAULT_SHUTDOWN_HOOK_PRIORITY);
  }

  private void doCleanObjects() throws InterruptedException {
    // Clean Buckets first
    executor = Executors.newFixedThreadPool(threadPoolSize);
    for (int i = 0; i < numOfThreads; i++) {
      executor.execute(new BucketCleaner());
    }
    LongSupplier currentValue = numberOfBucketsCleaned::get;
    progressbar = new ProgressBar(System.out, totalBucketCount, currentValue);

    LOG.info("Starting clean progress bar Thread.");
    progressbar.start();

    try {
      // wait until all Buckets are cleaned or exception occurred.
      while ((numberOfBucketsCleaned.get() != totalBucketCount)
          && exception == null) {
        Thread.sleep(CHECK_INTERVAL_MILLIS);
      }
    } catch (InterruptedException e) {
      LOG.error("Failed to wait until all Buckets are cleaned", e);
      Thread.currentThread().interrupt();
    }

    executor.shutdown();
    executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);

    // Clean Volume after cleaning Bucket
    for (int v = 0; v < numOfVolumes; v++) {
      cleanVolume(v);
    }

    if (exception != null) {
      progressbar.terminate();
    } else {
      progressbar.shutdown();
    }
  }

  /**
   * Prints stats of {@link Freon} run to the PrintStream.
   *
   * @param out PrintStream
   */
  void printStats(PrintStream out) {
    long endTime = System.nanoTime() - startTime;
    String execTime = DurationFormatUtils
        .formatDuration(TimeUnit.NANOSECONDS.toMillis(endTime),
            DURATION_FORMAT);

    long volumeTime = TimeUnit.NANOSECONDS.toMillis(volumeCreationTime.get())
        / threadPoolSize;
    String prettyAverageVolumeTime =
        DurationFormatUtils.formatDuration(volumeTime, DURATION_FORMAT);

    long bucketTime = TimeUnit.NANOSECONDS.toMillis(bucketCreationTime.get())
        / threadPoolSize;
    String prettyAverageBucketTime =
        DurationFormatUtils.formatDuration(bucketTime, DURATION_FORMAT);

    long averageKeyCreationTime =
        TimeUnit.NANOSECONDS.toMillis(keyCreationTime.get())
            / threadPoolSize;
    String prettyAverageKeyCreationTime = DurationFormatUtils
        .formatDuration(averageKeyCreationTime, DURATION_FORMAT);

    long averageKeyWriteTime =
        TimeUnit.NANOSECONDS.toMillis(keyWriteTime.get()) / threadPoolSize;
    String prettyAverageKeyWriteTime = DurationFormatUtils
        .formatDuration(averageKeyWriteTime, DURATION_FORMAT);

    out.println();
    out.println("***************************************************");
    out.println("Status: " + (exception != null ? "Failed" : "Success"));
    out.println("Git Base Revision: " + VersionInfo.getRevision());
    out.println("Number of Volumes created: " + numberOfVolumesCreated);
    out.println("Number of Buckets created: " + numberOfBucketsCreated);
    out.println("Number of Keys added: " + numberOfKeysAdded);
    if (replicationConfig != null) {
      out.println("Replication: " + replicationConfig);
    }
    out.println(
        "Average Time spent in volume creation: " + prettyAverageVolumeTime);
    out.println(
        "Average Time spent in bucket creation: " + prettyAverageBucketTime);
    out.println(
        "Average Time spent in key creation: " + prettyAverageKeyCreationTime);
    out.println(
        "Average Time spent in key write: " + prettyAverageKeyWriteTime);
    out.println("Total bytes written: " + totalBytesWritten);
    if (validateWrites) {
      out.println("Total number of writes validated: " +
          totalWritesValidated);
      out.println("Writes validated: " +
          (100.0 * totalWritesValidated.get() / numberOfKeysAdded.get())
          + " %");
      out.println("Successful validation: " +
          writeValidationSuccessCount);
      out.println("Unsuccessful validation: " +
          writeValidationFailureCount);
    }
    out.println("Total Execution time: " + execTime);
    out.println("***************************************************");

    if (jsonDir != null) {

      String[][] quantileTime =
          new String[FreonOps.values().length][QUANTILES + 1];
      String[] deviations = new String[FreonOps.values().length];
      String[] means = new String[FreonOps.values().length];
      for (FreonOps ops : FreonOps.values()) {
        Snapshot snapshot = histograms.get(ops.ordinal()).getSnapshot();
        for (int i = 0; i <= QUANTILES; i++) {
          quantileTime[ops.ordinal()][i] = DurationFormatUtils.formatDuration(
              TimeUnit.NANOSECONDS
                  .toMillis((long) snapshot.getValue((1.0 / QUANTILES) * i)),
              DURATION_FORMAT);
        }
        deviations[ops.ordinal()] = DurationFormatUtils.formatDuration(
            TimeUnit.NANOSECONDS.toMillis((long) snapshot.getStdDev()),
            DURATION_FORMAT);
        means[ops.ordinal()] = DurationFormatUtils.formatDuration(
            TimeUnit.NANOSECONDS.toMillis((long) snapshot.getMean()),
            DURATION_FORMAT);
      }

      FreonJobInfo jobInfo = new FreonJobInfo().setExecTime(execTime)
          .setGitBaseRevision(VersionInfo.getRevision())
          .setMeanVolumeCreateTime(means[FreonOps.VOLUME_CREATE.ordinal()])
          .setDeviationVolumeCreateTime(
              deviations[FreonOps.VOLUME_CREATE.ordinal()])
          .setTenQuantileVolumeCreateTime(
              quantileTime[FreonOps.VOLUME_CREATE.ordinal()])
          .setMeanBucketCreateTime(means[FreonOps.BUCKET_CREATE.ordinal()])
          .setDeviationBucketCreateTime(
              deviations[FreonOps.BUCKET_CREATE.ordinal()])
          .setTenQuantileBucketCreateTime(
              quantileTime[FreonOps.BUCKET_CREATE.ordinal()])
          .setMeanKeyCreateTime(means[FreonOps.KEY_CREATE.ordinal()])
          .setDeviationKeyCreateTime(deviations[FreonOps.KEY_CREATE.ordinal()])
          .setTenQuantileKeyCreateTime(
              quantileTime[FreonOps.KEY_CREATE.ordinal()])
          .setMeanKeyWriteTime(means[FreonOps.KEY_WRITE.ordinal()])
          .setDeviationKeyWriteTime(deviations[FreonOps.KEY_WRITE.ordinal()])
          .setTenQuantileKeyWriteTime(
              quantileTime[FreonOps.KEY_WRITE.ordinal()]);
      String jsonName =
          new SimpleDateFormat("yyyyMMddHHmmss").format(Time.now()) + ".json";
      String jsonPath = jsonDir + "/" + jsonName;
      try (OutputStream os = Files.newOutputStream(Paths.get(jsonPath))) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setVisibility(PropertyAccessor.FIELD,
            JsonAutoDetect.Visibility.ANY);
        ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
        writer.writeValue(os, jobInfo);
      } catch (FileNotFoundException | NoSuchFileException e) {
        out.println("Json File could not be created for the path: " + jsonPath);
        out.println(e);
      } catch (IOException e) {
        out.println("Json object could not be created");
        out.println(e);
      }
    }
  }

  /**
   * Returns the number of volumes created.
   *
   * @return volume count.
   */
  @VisibleForTesting
  int getNumberOfVolumesCreated() {
    return numberOfVolumesCreated.get();
  }

  /**
   * Returns the number of buckets created.
   *
   * @return bucket count.
   */
  @VisibleForTesting
  int getNumberOfBucketsCreated() {
    return numberOfBucketsCreated.get();
  }

  /**
   * Returns the number of keys added.
   *
   * @return keys count.
   */
  @VisibleForTesting
  long getNumberOfKeysAdded() {
    return numberOfKeysAdded.get();
  }

  /**
   * Returns the number of volumes cleaned.
   *
   * @return cleaned volume count.
   */
  @VisibleForTesting
  int getNumberOfVolumesCleaned() {
    return numberOfVolumesCleaned.get();
  }

  /**
   * Returns the number of buckets cleaned.
   *
   * @return cleaned bucket count.
   */
  @VisibleForTesting
  int getNumberOfBucketsCleaned() {
    return numberOfBucketsCleaned.get();
  }

  /**
   * Returns true if random validation of write is enabled.
   *
   * @return validateWrites
   */
  @VisibleForTesting
  boolean getValidateWrites() {
    return validateWrites;
  }

  /**
   * Returns the number of keys validated.
   *
   * @return validated key count.
   */
  @VisibleForTesting
  long getTotalKeysValidated() {
    return totalWritesValidated.get();
  }

  /**
   * Returns the number of successful validation.
   *
   * @return successful validation count.
   */
  @VisibleForTesting
  long getSuccessfulValidationCount() {
    return writeValidationSuccessCount.get();
  }

  /**
   * Returns the number of unsuccessful validation.
   *
   * @return unsuccessful validation count.
   */
  @VisibleForTesting
  long getUnsuccessfulValidationCount() {
    return validateWrites ? writeValidationFailureCount.get() : 0;
  }

  /**
   * Returns the current size of the buckets map.
   *
   * @return number of buckets created and added to the map
   */
  @VisibleForTesting
  int getBucketMapSize() {
    return buckets.size();
  }

  /**
   * Wrapper to hold ozone keyValidate entry.
   */
  private static class KeyValidate {
    /**
     * Bucket name.
     */
    private OzoneBucket bucket;

    /**
     * Key name.
     */
    private String keyName;

    /**
     * Digest of this key's full value.
     */
    private byte[] digest;

    /**
     * Constructs a new ozone keyValidate.
     *
     * @param bucket    bucket part
     * @param keyName   key part
     * @param digest    digest of this key's full value
     */
    KeyValidate(OzoneBucket bucket, String keyName, byte[] digest) {
      this.bucket = bucket;
      this.keyName = keyName;
      this.digest = digest;
    }
  }

  private class ObjectCreator implements Runnable {
    @Override
    public void run() {
      int v;
      while ((v = volumeCounter.getAndIncrement()) < numOfVolumes) {
        if (!createVolume(v)) {
          return;
        }
      }

      int b;
      while ((b = bucketCounter.getAndIncrement()) < totalBucketCount) {
        if (!createBucket(b)) {
          return;
        }
      }

      long k;
      while ((k = keyCounter.getAndIncrement()) < totalKeyCount) {
        if (!createKey(k)) {
          return;
        }
      }
    }
  }

  private class BucketCleaner implements Runnable {
    @Override
    public void run() {
      int b;
      while ((b = cleanedBucketCounter.getAndIncrement()) < totalBucketCount) {
        if (!cleanBucket(b)) {
          return;
        }
      }
    }
  }

  private boolean createVolume(int volumeNumber) {
    String volumeName = "vol-" + volumeNumber + "-"
        + RandomStringUtils.secure().nextNumeric(5);
    LOG.trace("Creating volume: {}", volumeName);
    try (TracingUtil.TraceCloseable scope = TracingUtil
        .createActivatedSpan("createVolume")) {
      long start = System.nanoTime();
      objectStore.createVolume(volumeName);
      long volumeCreationDuration = System.nanoTime() - start;
      volumeCreationTime.getAndAdd(volumeCreationDuration);
      histograms.get(FreonOps.VOLUME_CREATE.ordinal())
          .update(volumeCreationDuration);
      numberOfVolumesCreated.getAndIncrement();

      OzoneVolume volume = objectStore.getVolume(volumeName);
      volumes.put(volumeNumber, volume);
      return true;
    } catch (Throwable e) {
      exception = e;
      LOG.error("Could not create volume", e);
      return false;
    }
  }

  private boolean createBucket(int globalBucketNumber) {
    int volumeNumber = globalBucketNumber % numOfVolumes;
    int bucketNumber = globalBucketNumber / numOfVolumes;
    OzoneVolume volume = getVolume(volumeNumber);
    if (volume == null) {
      LOG.error("Could not find volume {}", volumeNumber);
      return false;
    }
    String bucketName = "bucket-" + bucketNumber + "-" +
        RandomStringUtils.secure().nextNumeric(5);
    LOG.trace("Creating bucket: {} in volume: {}",
        bucketName, volume.getName());
    try (TracingUtil.TraceCloseable scope = TracingUtil
        .createActivatedSpan("createBucket")) {

      long start = System.nanoTime();
      if (bucketLayout != null) {
        BucketArgs bucketArgs = BucketArgs.newBuilder()
            .setBucketLayout(bucketLayout)
            .build();
        volume.createBucket(bucketName, bucketArgs);
      } else {
        volume.createBucket(bucketName);
      }
      long bucketCreationDuration = System.nanoTime() - start;
      histograms.get(FreonOps.BUCKET_CREATE.ordinal())
          .update(bucketCreationDuration);
      bucketCreationTime.getAndAdd(bucketCreationDuration);
      numberOfBucketsCreated.getAndIncrement();

      OzoneBucket bucket = volume.getBucket(bucketName);
      buckets.put(globalBucketNumber, bucket);
      return true;
    } catch (Throwable e) {
      exception = e;
      LOG.error("Could not create bucket ", e);
      return false;
    }
  }

  private boolean createKey(long globalKeyNumber) {
    int globalBucketNumber = (int) (globalKeyNumber % totalBucketCount);
    long keyNumber = globalKeyNumber / totalBucketCount;
    OzoneBucket bucket = getBucket(globalBucketNumber);
    if (bucket == null) {
      LOG.error("Could not find bucket {}", globalBucketNumber);
      return false;
    }
    String bucketName = bucket.getName();
    String volumeName = bucket.getVolumeName();
    String keyName = "key-" + keyNumber + "-"
        + RandomStringUtils.secure().nextNumeric(5);
    LOG.trace("Adding key: {} in bucket: {} of volume: {}",
        keyName, bucketName, volumeName);
    try {
      try (TracingUtil.TraceCloseable scope = TracingUtil.createActivatedSpan("createKey")) {
        long keyCreateStart = System.nanoTime();
        try (OzoneOutputStream os = bucket.createKey(keyName, keySize.toBytes(),
            replicationConfig, new HashMap<>())) {
          long keyCreationDuration = System.nanoTime() - keyCreateStart;
          histograms.get(FreonOps.KEY_CREATE.ordinal())
              .update(keyCreationDuration);
          keyCreationTime.getAndAdd(keyCreationDuration);

          try (AutoCloseable writeScope = TracingUtil
              .createActivatedSpan("writeKeyData")) {
            long keyWriteStart = System.nanoTime();
            for (long nrRemaining = keySize.toBytes();
                 nrRemaining > 0; nrRemaining -= bufferSize) {
              int curSize = (int) Math.min(bufferSize, nrRemaining);
              os.write(keyValueBuffer, 0, curSize);
            }

            long keyWriteDuration = System.nanoTime() - keyWriteStart;
            histograms.get(FreonOps.KEY_WRITE.ordinal())
                .update(keyWriteDuration);
            keyWriteTime.getAndAdd(keyWriteDuration);
            totalBytesWritten.getAndAdd(keySize.toBytes());
            numberOfKeysAdded.getAndIncrement();
          }
        }
      }

      if (validateWrites) {
        MessageDigest tmpMD = (MessageDigest) commonInitialMD.clone();
        boolean validate = validationQueue.offer(
            new KeyValidate(bucket, keyName, tmpMD.digest()));
        if (validate) {
          LOG.trace("Key {} is queued for validation.", keyName);
        }
      }

      return true;
    } catch (Throwable e) {
      exception = e;
      LOG.error("Exception while adding key: {} in bucket: {}" +
          " of volume: {}.", keyName, bucketName, volumeName, e);
      return false;
    }
  }

  private boolean cleanVolume(int volumeNumber) {
    OzoneVolume volume = getVolume(volumeNumber);
    String volumeName = volume.getName();
    LOG.trace("Cleaning volume: {}", volumeName);
    try (TracingUtil.TraceCloseable scope = TracingUtil
        .createActivatedSpan("cleanVolume")) {
      objectStore.deleteVolume(volumeName);
      numberOfVolumesCleaned.getAndIncrement();
      return true;
    } catch (Throwable e) {
      exception = e;
      LOG.error("Could not clean volume", e);
      return false;
    }
  }

  private boolean cleanBucket(int globalBucketNumber) {
    int volumeNumber = globalBucketNumber % numOfVolumes;
    OzoneVolume volume = getVolume(volumeNumber);
    OzoneBucket bucket = getBucket(globalBucketNumber);
    String bucketName = bucket.getName();
    if (volume == null) {
      LOG.error("Could not find volume {}", volumeNumber);
      return false;
    }
    LOG.trace("Cleaning bucket: {} in volume: {}",
        bucketName, volume.getName());
    ArrayList<String> keys = new ArrayList<>();
    try {
      bucket.listKeys(null).forEachRemaining(x -> keys.add(x.getName()));
      bucket.deleteKeys(keys);
      volume.deleteBucket(bucketName);
      numberOfBucketsCleaned.getAndIncrement();
      return true;
    } catch (Throwable e) {
      exception = e;
      LOG.error("Could not clean bucket ", e);
      return false;
    }
  }

  private OzoneVolume getVolume(Integer volumeNumber) {
    return waitUntilAddedToMap(volumes, volumeNumber);
  }

  @VisibleForTesting
  OzoneBucket getBucket(Integer bucketNumber) {
    return waitUntilAddedToMap(buckets, bucketNumber);
  }

  /**
   * Looks up volume or bucket from the cache.  Waits for it to be created if
   * needed (can happen for the last few items depending on the number of
   * threads).
   *
   * @return may return null if this thread is interrupted, or if any other
   *   thread encounters an exception (and stores it to {@code exception})
   */
  private <T> T waitUntilAddedToMap(Map<Integer, T> map, Integer i) {
    while (exception == null && !map.containsKey(i)) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return null;
      }
    }
    return map.get(i);
  }

  private final class FreonJobInfo {

    private String status;
    private String gitBaseRevision;
    private String jobStartTime;
    private int numOfVolumes;
    private int numOfBuckets;
    private int numOfKeys;
    private int numOfThreads;
    private String dataWritten;
    private String execTime;
    private String replication;
    private String replicationType;

    private long keySize;
    private int bufferSize;

    private String totalThroughputPerSecond;

    private String meanVolumeCreateTime;
    private String deviationVolumeCreateTime;
    private String[] tenQuantileVolumeCreateTime;

    private String meanBucketCreateTime;
    private String deviationBucketCreateTime;
    private String[] tenQuantileBucketCreateTime;

    private String meanKeyCreateTime;
    private String deviationKeyCreateTime;
    private String[] tenQuantileKeyCreateTime;

    private String meanKeyWriteTime;
    private String deviationKeyWriteTime;
    private String[] tenQuantileKeyWriteTime;

    private FreonJobInfo() {
      this.status = exception != null ? "Failed" : "Success";
      this.numOfVolumes = RandomKeyGenerator.this.numOfVolumes;
      this.numOfBuckets = RandomKeyGenerator.this.numOfBuckets;
      this.numOfKeys = RandomKeyGenerator.this.numOfKeys;
      this.numOfThreads = RandomKeyGenerator.this.numOfThreads;
      this.keySize = RandomKeyGenerator.this.keySize.toBytes();
      this.bufferSize = RandomKeyGenerator.this.bufferSize;
      this.jobStartTime = Time.formatTime(RandomKeyGenerator.this.jobStartTime);
      replicationType = replicationConfig.getReplicationType().name();
      replication = replicationConfig.getReplication();

      long totalBytes =
          (long) numOfVolumes * numOfBuckets * numOfKeys * keySize;
      this.dataWritten = getInStorageUnits((double) totalBytes);
      this.totalThroughputPerSecond = getInStorageUnits(
          (totalBytes * 1.0) / TimeUnit.NANOSECONDS
              .toSeconds(
                  RandomKeyGenerator.this.keyWriteTime.get() / threadPoolSize));
    }

    private String getInStorageUnits(Double value) {
      double size;
      OzoneConsts.Units unit;
      if ((long) (value / OzoneConsts.TB) != 0) {
        size = value / OzoneConsts.TB;
        unit = OzoneConsts.Units.TB;
      } else if ((long) (value / OzoneConsts.GB) != 0) {
        size = value / OzoneConsts.GB;
        unit = OzoneConsts.Units.GB;
      } else if ((long) (value / OzoneConsts.MB) != 0) {
        size = value / OzoneConsts.MB;
        unit = OzoneConsts.Units.MB;
      } else if ((long) (value / OzoneConsts.KB) != 0) {
        size = value / OzoneConsts.KB;
        unit = OzoneConsts.Units.KB;
      } else {
        size = value;
        unit = OzoneConsts.Units.B;
      }
      return size + " " + unit;
    }

    public FreonJobInfo setGitBaseRevision(String gitBaseRevisionVal) {
      gitBaseRevision = gitBaseRevisionVal;
      return this;
    }

    public FreonJobInfo setExecTime(String execTimeVal) {
      execTime = execTimeVal;
      return this;
    }

    public FreonJobInfo setMeanKeyWriteTime(String deviationKeyWriteTimeVal) {
      this.meanKeyWriteTime = deviationKeyWriteTimeVal;
      return this;
    }

    public FreonJobInfo setDeviationKeyWriteTime(
        String deviationKeyWriteTimeVal) {
      this.deviationKeyWriteTime = deviationKeyWriteTimeVal;
      return this;
    }

    public FreonJobInfo setTenQuantileKeyWriteTime(
        String[] tenQuantileKeyWriteTimeVal) {
      this.tenQuantileKeyWriteTime = tenQuantileKeyWriteTimeVal;
      return this;
    }

    public FreonJobInfo setMeanKeyCreateTime(String deviationKeyWriteTimeVal) {
      this.meanKeyCreateTime = deviationKeyWriteTimeVal;
      return this;
    }

    public FreonJobInfo setDeviationKeyCreateTime(
        String deviationKeyCreateTimeVal) {
      this.deviationKeyCreateTime = deviationKeyCreateTimeVal;
      return this;
    }

    public FreonJobInfo setTenQuantileKeyCreateTime(
        String[] tenQuantileKeyCreateTimeVal) {
      this.tenQuantileKeyCreateTime = tenQuantileKeyCreateTimeVal;
      return this;
    }

    public FreonJobInfo setMeanBucketCreateTime(
        String deviationKeyWriteTimeVal) {
      this.meanBucketCreateTime = deviationKeyWriteTimeVal;
      return this;
    }

    public FreonJobInfo setDeviationBucketCreateTime(
        String deviationBucketCreateTimeVal) {
      this.deviationBucketCreateTime = deviationBucketCreateTimeVal;
      return this;
    }

    public FreonJobInfo setTenQuantileBucketCreateTime(
        String[] tenQuantileBucketCreateTimeVal) {
      this.tenQuantileBucketCreateTime = tenQuantileBucketCreateTimeVal;
      return this;
    }

    public FreonJobInfo setMeanVolumeCreateTime(
        String deviationKeyWriteTimeVal) {
      this.meanVolumeCreateTime = deviationKeyWriteTimeVal;
      return this;
    }

    public FreonJobInfo setDeviationVolumeCreateTime(
        String deviationVolumeCreateTimeVal) {
      this.deviationVolumeCreateTime = deviationVolumeCreateTimeVal;
      return this;
    }

    public FreonJobInfo setTenQuantileVolumeCreateTime(
        String[] tenQuantileVolumeCreateTimeVal) {
      this.tenQuantileVolumeCreateTime = tenQuantileVolumeCreateTimeVal;
      return this;
    }

    public String getJobStartTime() {
      return jobStartTime;
    }

    public int getNumOfVolumes() {
      return numOfVolumes;
    }

    public int getNumOfBuckets() {
      return numOfBuckets;
    }

    public int getNumOfKeys() {
      return numOfKeys;
    }

    public int getNumOfThreads() {
      return numOfThreads;
    }

    public String getExecTime() {
      return execTime;
    }

    public String getReplication() {
      return replication;
    }

    public String getReplicationType() {
      return replicationType;
    }

    public String getStatus() {
      return status;
    }

    public long getKeySize() {
      return keySize;
    }

    public int getBufferSize() {
      return bufferSize;
    }

    public String getGitBaseRevision() {
      return gitBaseRevision;
    }

    public String getDataWritten() {
      return dataWritten;
    }

    public String getTotalThroughputPerSecond() {
      return totalThroughputPerSecond;
    }

    public String getMeanVolumeCreateTime() {
      return meanVolumeCreateTime;
    }

    public String getDeviationVolumeCreateTime() {
      return deviationVolumeCreateTime;
    }

    public String[] getTenQuantileVolumeCreateTime() {
      return tenQuantileVolumeCreateTime;
    }

    public String getMeanBucketCreateTime() {
      return meanBucketCreateTime;
    }

    public String getDeviationBucketCreateTime() {
      return deviationBucketCreateTime;
    }

    public String[] getTenQuantileBucketCreateTime() {
      return tenQuantileBucketCreateTime;
    }

    public String getMeanKeyCreateTime() {
      return meanKeyCreateTime;
    }

    public String getDeviationKeyCreateTime() {
      return deviationKeyCreateTime;
    }

    public String[] getTenQuantileKeyCreateTime() {
      return tenQuantileKeyCreateTime;
    }

    public String getMeanKeyWriteTime() {
      return meanKeyWriteTime;
    }

    public String getDeviationKeyWriteTime() {
      return deviationKeyWriteTime;
    }

    public String[] getTenQuantileKeyWriteTime() {
      return tenQuantileKeyWriteTime;
    }
  }

  /**
   * Validates the write done in ozone cluster.
   */
  private class Validator implements Runnable {
    @Override
    public void run() {
      DigestUtils dig = new DigestUtils(DIGEST_ALGORITHM);

      while (true) {
        if (completed && validationQueue.isEmpty()) {
          return;
        }

        try {
          KeyValidate kv = validationQueue.poll(5, TimeUnit.SECONDS);
          if (kv != null) {
            try (OzoneInputStream is = kv.bucket.readKey(kv.keyName)) {
              dig.getMessageDigest().reset();
              byte[] curDigest = dig.digest(is);
              totalWritesValidated.getAndIncrement();
              if (MessageDigest.isEqual(kv.digest, curDigest)) {
                writeValidationSuccessCount.getAndIncrement();
              } else {
                writeValidationFailureCount.getAndIncrement();
                LOG.warn("Data validation error for key {}/{}/{}",
                    kv.bucket.getVolumeName(), kv.bucket, kv.keyName);
                LOG.warn("Expected checksum: {}, Actual checksum: {}",
                    kv.digest, curDigest);
              }
            }
          }
        } catch (IOException ex) {
          LOG.error("Exception while validating write.", ex);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  @VisibleForTesting
  public int getThreadPoolSize() {
    return threadPoolSize;
  }

  enum FreonOps {
    VOLUME_CREATE,
    BUCKET_CREATE,
    KEY_CREATE,
    KEY_WRITE
  }
}
