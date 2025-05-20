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

package org.apache.hadoop.ozone.debug.replicas;

import static com.amazonaws.protocol.json.SdkStructuredPlainJsonFactory.JSON_FACTORY;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.cli.ScmOption;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.Shell;
import picocli.CommandLine;

/**
 * Verify replicas command.
 */

@CommandLine.Command(
    name = "verify",
    description = "Run checks to verify data across replicas. By default prints only the keys with failed checks.")
public class ReplicasVerify extends Handler {
  static class Verification {
    @CommandLine.Option(names = "--checksums",
        description = "Do client side data checksum validation of all replicas.",
        // value will be true only if the "--checksums" option was specified on the CLI
        defaultValue = "false")
    private boolean doExecuteChecksums;

    @CommandLine.Option(names = "--block-existence",
        description = "Check for block existence on datanodes.",
        defaultValue = "false")
    private boolean doExecuteBlockExistence;
  }

  @CommandLine.Mixin
  private ScmOption scmOption;

  @CommandLine.Parameters(arity = "1",
      description = Shell.OZONE_URI_DESCRIPTION)
  private String uri;

  @CommandLine.Option(names = {"-o", "--output-dir"},
      description = "Destination directory to save the generated output.",
      required = true)
  private String outputDir;

  @CommandLine.Option(names = {"--all-results"},
      description = "Print results for all passing and failing keys")
  private boolean allResults;

  @CommandLine.ArgGroup(exclusive = false, multiplicity = "1")
  private Verification verification;

  @CommandLine.Option(names = "--threads",
      description = "Number of threads to use for verification",
      defaultValue = "10")
  private int threadCount;

  private ExecutorService verificationExecutor;
  private ExecutorService writerExecutor;
  private ThreadLocal<List<ReplicaVerifier>> threadLocalVerifiers;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      .configure(SerializationFeature.INDENT_OUTPUT, true);
  private static final ObjectWriter WRITER = OBJECT_MAPPER.writer();

  @Override
  protected void execute(OzoneClient client, OzoneAddress address) throws IOException {
    if (threadCount < 1 || threadCount > 100) {
      LOG.error("Thread count must be between 1 and 100");
      return;
    }
    verificationExecutor =  new ThreadPoolExecutor(threadCount, threadCount, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(2 * threadCount), new ThreadPoolExecutor.CallerRunsPolicy());
    writerExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(threadCount), new ThreadPoolExecutor.CallerRunsPolicy());
    threadLocalVerifiers = ThreadLocal.withInitial(() -> {
      List<ReplicaVerifier> verifiers = new ArrayList<>();
      try {
        if (verification.doExecuteChecksums) {
          verifiers.add(new ChecksumVerifier(getConf()));
        }

        if (verification.doExecuteBlockExistence) {
          verifiers.add(new BlockExistenceVerifier(getConf()));
        }
      } catch (IOException e) {
        LOG.error("Error initializing verifiers", e);
        throw new RuntimeException("Error initializing verifiers", e);
      }
      return verifiers;
    });

    findCandidateKeys(client, address);
  }

  @Override
  protected OzoneAddress getAddress() throws OzoneClientException {
    return new OzoneAddress(uri);
  }

  void findCandidateKeys(OzoneClient ozoneClient, OzoneAddress address) {
    ObjectStore objectStore = ozoneClient.getObjectStore();
    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    String keyName = address.getKeyName();

    AtomicBoolean allKeysPassed = new AtomicBoolean(true);

    try (JsonGenerator jsonGenerator = JSON_FACTORY.createGenerator(System.out, JsonEncoding.UTF8)) {
      // open json
      jsonGenerator.useDefaultPrettyPrinter();
      jsonGenerator.writeStartObject();
      jsonGenerator.writeFieldName("keys");
      jsonGenerator.writeStartArray();
      jsonGenerator.flush();

      // Process keys based on the provided address
      try (SequenceWriter sequenceWriter = createSequenceWriter(false, jsonGenerator)) {
        if (!keyName.isEmpty()) {
          processKey(ozoneClient, volumeName, bucketName, keyName, sequenceWriter, allKeysPassed);
        } else if (!bucketName.isEmpty()) {
          OzoneVolume volume = objectStore.getVolume(volumeName);
          OzoneBucket bucket = volume.getBucket(bucketName);
          checkBucket(ozoneClient, bucket, sequenceWriter, allKeysPassed);
        } else if (!volumeName.isEmpty()) {
          OzoneVolume volume = objectStore.getVolume(volumeName);
          checkVolume(ozoneClient, volume, sequenceWriter, allKeysPassed);
        } else {
          for (Iterator<? extends OzoneVolume> it = objectStore.listVolumes(null); it.hasNext();) {
            checkVolume(ozoneClient, it.next(), sequenceWriter, allKeysPassed);
          }
        }
      } catch (IOException e) {
        LOG.error("Error while verifying keys", e);
      } finally {
        verificationExecutor.shutdown();
        writerExecutor.shutdown();
        try {
          // Wait for all tasks to complete
          if (!verificationExecutor.awaitTermination(1, TimeUnit.DAYS) ||
              !writerExecutor.awaitTermination(1, TimeUnit.DAYS)) {
            LOG.warn("Failed to wait for all tasks to complete");
          }
          threadLocalVerifiers.remove();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Interrupted while waiting for verification to complete", e);
        }
      }

      // close json
      jsonGenerator.writeEndArray();
      jsonGenerator.writeBooleanField("pass", allKeysPassed.get());
      jsonGenerator.writeEndObject();
    } catch (IOException e) {
      LOG.error("Error generating verification result", e);
    }
  }

  void checkVolume(OzoneClient ozoneClient, OzoneVolume volume,
      SequenceWriter sequenceWriter, AtomicBoolean allKeysPassed) {
    for (Iterator<? extends OzoneBucket> it = volume.listBuckets(null); it.hasNext();) {
      OzoneBucket bucket = it.next();
      checkBucket(ozoneClient, bucket, sequenceWriter, allKeysPassed);
    }
  }

  void checkBucket(OzoneClient ozoneClient, OzoneBucket bucket,
      SequenceWriter sequenceWriter, AtomicBoolean allKeysPassed) {
    try {
      for (Iterator<? extends OzoneKey> it = bucket.listKeys(null); it.hasNext();) {
        OzoneKey key = it.next();
        // TODO: Remove this check once HDDS-12094 is fixed
        if (!key.getName().endsWith("/")) {
          processKey(ozoneClient, key.getVolumeName(), key.getBucketName(),
              key.getName(), sequenceWriter, allKeysPassed);
        }
      }
    } catch (IOException e) {
      LOG.error("Error processing bucket {}/{}", bucket.getVolumeName(), bucket.getName(), e);
    }
  }

  private void processKey(OzoneClient ozoneClient, String volumeName, String bucketName,
      String keyName, SequenceWriter sequenceWriter, AtomicBoolean allKeysPassed) {
    OmKeyInfo keyInfo;
    try {
      keyInfo = ozoneClient.getProxy().getKeyInfo(volumeName, bucketName, keyName, false);
    } catch (IOException e) {
      LOG.error("Error processing key {}/{}/{}", volumeName, bucketName, keyName, e);
      return;
    }

    CompletableFuture.supplyAsync(() -> verifyKey(volumeName, bucketName, keyName, keyInfo), verificationExecutor)
        .thenAcceptAsync(keyResult ->
            writeVerificationResult(sequenceWriter, allKeysPassed, keyResult), writerExecutor);
  }

  private synchronized void writeVerificationResult(SequenceWriter sequenceWriter,
      AtomicBoolean allKeysPassed, KeyVerificationResult keyResult) {
    try {
      allKeysPassed.compareAndSet(true, keyResult.isKeyPass());
      if (!keyResult.isKeyPass() || allResults) {
        ObjectNode keyNode = OBJECT_MAPPER.convertValue(keyResult, ObjectNode.class);
        sequenceWriter.write(keyNode);
      }
    } catch (IOException e) {
      LOG.error("Error writing verification result", e);
      throw new CompletionException(e);
    }
  }

  private KeyVerificationResult verifyKey(String volumeName, String bucketName, String keyName, OmKeyInfo keyInfo) {
    boolean keyPass = true;
    List<KeyVerificationResult.BlockVerificationData> blockResults = new ArrayList<>();
    List<ReplicaVerifier> localVerifiers = threadLocalVerifiers.get();

    for (OmKeyLocationInfo keyLocation : Objects.requireNonNull(keyInfo.getLatestVersionLocations())
        .getBlocksLatestVersionOnly()) {
      long containerID = keyLocation.getContainerID();
      long localID = keyLocation.getLocalID();

      List<KeyVerificationResult.ReplicaVerificationData> replicaResults = new ArrayList<>();
      boolean blockPass = true;

      for (DatanodeDetails datanode : keyLocation.getPipeline().getNodes()) {
        List<KeyVerificationResult.CheckData> checkResults = new ArrayList<>();
        boolean replicaPass = true;
        int replicaIndex = keyLocation.getPipeline().getReplicaIndex(datanode);

        for (ReplicaVerifier verifier : localVerifiers) {
          BlockVerificationResult result = verifier.verifyBlock(datanode, keyLocation, replicaIndex);
          KeyVerificationResult.CheckData checkResult = new KeyVerificationResult.CheckData(verifier.getType(),
              result.isCompleted(), result.passed(), result.getFailures());
          checkResults.add(checkResult);

          if (!result.passed()) {
            replicaPass = false;
          }
        }

        KeyVerificationResult.ReplicaVerificationData replicaResult =
            new KeyVerificationResult.ReplicaVerificationData(datanode, replicaIndex, checkResults, replicaPass);
        replicaResults.add(replicaResult);

        if (!replicaPass) {
          blockPass = false;
        }
      }

      KeyVerificationResult.BlockVerificationData blockResult =
          new KeyVerificationResult.BlockVerificationData(containerID, localID, replicaResults, blockPass);
      blockResults.add(blockResult);

      if (!blockPass) {
        keyPass = false;
      }
    }

    return new KeyVerificationResult(volumeName, bucketName, keyName, blockResults, keyPass);
  }

  private SequenceWriter createSequenceWriter(boolean doWrapinArray, JsonGenerator jsonGenerator) throws IOException {
    SequenceWriter sequenceWriter = WRITER.writeValues(jsonGenerator);
    sequenceWriter.init(doWrapinArray);
    return sequenceWriter;
  }
}
