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

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;

import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import static com.amazonaws.services.s3.internal.SkipMd5CheckStrategy.DISABLE_PUT_OBJECT_MD5_VALIDATION_PROPERTY;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.codahale.metrics.Timer;
import org.apache.commons.lang3.RandomStringUtils;

import static org.apache.hadoop.ozone.OzoneConsts.OM_MULTIPART_MIN_SIZE;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Generate random keys via the s3 interface.
 */
@Command(name = "s3kg",
    aliases = "s3-key-generator",
    description = "Create random keys via the s3 interface." +
        "Command requires AWS authentication environment " +
        "variables set, AWS_ACCESS_KEY_ID and " +
        "AWS_SECRET_ACCESS_KEY",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@SuppressWarnings("java:S2245") // no need for secure random
public class S3KeyGenerator extends S3EntityGenerator
    implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3KeyGenerator.class);

  @Option(names = {"-b", "--bucket"},
      description =
          "Name of the (S3!) bucket which contains the test data.",
      defaultValue = "bucket1")
  private String bucketName;

  @Option(names = {"-s", "--size"},
      description = "Size of the generated key (in bytes) or size of one "
          + "multipart upload part (in case of multipart upload)",
      defaultValue = "10240")
  private int fileSize;

  @Option(names = {"--multi-part-upload"},
      description = "User multi part upload",
      defaultValue = "false")
  private boolean multiPart;

  @Option(names = {"--parts"},
      description = "Number of parts for multipart upload (final size = "
          + "--size * --parts)",
      defaultValue = "10")
  private int numberOfParts;

  private Timer timer;

  private String content;

  @Override
  public Void call() throws Exception {

    if (multiPart && fileSize < OM_MULTIPART_MIN_SIZE) {
      throw new IllegalArgumentException(
          "Size of multipart upload parts should be at least 5MB (5242880)");
    }

    s3ClientInit();

    content = RandomStringUtils.randomAscii(fileSize);

    timer = getMetrics().timer("key-create");

    System.setProperty(DISABLE_PUT_OBJECT_MD5_VALIDATION_PROPERTY, "true");
    runTests(this::createKey);

    return null;
  }

  private void createKey(long counter) throws Exception {
    timer.time(() -> {
      if (multiPart) {

        final String keyName = generateObjectName(counter);
        final InitiateMultipartUploadRequest initiateRequest =
            new InitiateMultipartUploadRequest(bucketName, keyName);

        final InitiateMultipartUploadResult initiateMultipartUploadResult =
            getS3().initiateMultipartUpload(initiateRequest);
        final String uploadId = initiateMultipartUploadResult.getUploadId();

        List<PartETag> parts = new ArrayList<>();
        for (int i = 1; i <= numberOfParts; i++) {

          final UploadPartRequest uploadPartRequest = new UploadPartRequest()
              .withBucketName(bucketName)
              .withKey(keyName)
              .withPartNumber(i)
              .withLastPart(i == numberOfParts)
              .withUploadId(uploadId)
              .withPartSize(fileSize)
              .withInputStream(new ByteArrayInputStream(content.getBytes(
                  StandardCharsets.UTF_8)));

          final UploadPartResult uploadPartResult =
                  getS3().uploadPart(uploadPartRequest);
          parts.add(uploadPartResult.getPartETag());
        }

        getS3().completeMultipartUpload(
            new CompleteMultipartUploadRequest(bucketName, keyName, uploadId,
                parts));

      } else {
        getS3().putObject(bucketName, generateObjectName(counter),
            content);
      }

      return null;
    });
  }
}
