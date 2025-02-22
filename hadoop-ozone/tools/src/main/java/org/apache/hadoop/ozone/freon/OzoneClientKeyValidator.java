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

import com.codahale.metrics.Timer;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.concurrent.Callable;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.ratis.util.function.CheckedFunction;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Data generator tool test om performance.
 */
@Command(name = "ockv",
    aliases = "ozone-client-key-validator",
    description = "Validate keys with the help of the ozone clients.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
public class OzoneClientKeyValidator extends BaseFreonGenerator
    implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneClientKeyValidator.class);

  @Option(names = {"-v", "--volume"},
      description = "Name of the volume which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "vol1")
  private String volumeName;

  @Option(names = {"-b", "--bucket"},
      description = "Name of the bucket which contains the test data.",
      defaultValue = "bucket1")
  private String bucketName;

  @Option(names = {"-s", "--stream"},
      description = "Whether to calculate key digest during read from stream,"
          + " or separately after it is completely read.",
      defaultValue = "false")
  private boolean stream;

  @Option(
      names = "--om-service-id",
      description = "OM Service ID"
  )
  private String omServiceID = null;

  private Timer timer;

  private byte[] referenceDigest;
  private long referenceKeySize;

  private OzoneClient rpcClient;

  @Override
  public Void call() throws Exception {

    init();

    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();

    rpcClient = createOzoneClient(omServiceID, ozoneConfiguration);

    readReference();

    timer = getMetrics().timer("key-validate");

    runTests(this::validateKey);

    rpcClient.close();

    return null;
  }

  private void readReference() throws IOException {
    String name = generateObjectName(0);

    if (!stream) {
      // first obtain key size to be able to allocate exact buffer for keys
      referenceKeySize = getKeySize(name);

      // force stream if key is too large for byte[]
      // (limit taken from ByteArrayOutputStream)
      if (referenceKeySize > Integer.MAX_VALUE - 8) {
        LOG.warn("Forcing 'stream' option, as key size is too large: {} bytes",
            referenceKeySize);
        stream = true;
      }
    }

    if (stream) {
      referenceDigest = calculateDigestStreaming(name);
    } else {
      byte[] data = readKeyToByteArray(name);
      referenceDigest = getDigest(data);
    }
  }

  private long getKeySize(String keyName) throws IOException {
    return rpcClient.getObjectStore().getVolume(volumeName)
        .getBucket(bucketName).getKey(keyName).getDataSize();
  }

  private void validateKey(long counter) throws Exception {
    String objectName = generateObjectName(counter);
    byte[] digest = getDigest(objectName);
    validateDigest(objectName, digest);
  }

  private byte[] getDigest(String objectName) throws Exception {
    byte[] digest;
    if (stream) {
      // Calculating the digest during stream read requires only constant
      // memory, but timing results include digest calculation time, too.
      digest = timer.time(() ->
          calculateDigestStreaming(objectName));
    } else {
      byte[] data = timer.time(() -> readKeyToByteArray(objectName));
      digest = getDigest(data);
    }
    return digest;
  }

  private byte[] calculateDigestStreaming(String name) throws IOException {
    return readKey(name, BaseFreonGenerator::getDigest);
  }

  private byte[] readKeyToByteArray(String name) throws IOException {
    return readKey(name, in -> IOUtils.toByteArray(in, referenceKeySize));
  }

  private <T> T readKey(String keyName,
      CheckedFunction<InputStream, T, IOException> reader)
      throws IOException {
    try (InputStream in = rpcClient.getObjectStore().getVolume(volumeName)
        .getBucket(bucketName).readKey(keyName)) {
      return reader.apply(in);
    }
  }

  private void validateDigest(String objectName, byte[] digest) {
    if (!MessageDigest.isEqual(referenceDigest, digest)) {
      throw new IllegalStateException(
          "Reference (=first) message digest doesn't match with digest of "
              + objectName);
    }
  }

}
