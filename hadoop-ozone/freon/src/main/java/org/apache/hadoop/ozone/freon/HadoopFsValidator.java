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
import java.security.MessageDigest;
import java.util.concurrent.Callable;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;

/**
 * Data generator tool test om performance.
 */
@Command(name = "dfsv",
    aliases = "dfs-file-validator",
    description = "Validate if the generated files have the same hash.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
public class HadoopFsValidator extends HadoopBaseFreonGenerator
    implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(HadoopFsValidator.class);

  private ContentGenerator contentGenerator;

  private Timer timer;

  private byte[] referenceDigest;

  @Override
  public Void call() throws Exception {
    super.init();

    Path file = new Path(getRootPath() + "/" + generateObjectName(0));
    try (FSDataInputStream stream = getFileSystem().open(file)) {
      referenceDigest = getDigest(stream);
    }

    timer = getMetrics().timer("file-read");

    runTests(this::validateFile);

    return null;
  }

  private void validateFile(long counter) throws Exception {
    Path file = new Path(getRootPath() + "/" + generateObjectName(counter));

    byte[] content = timer.time(() -> {
      try (FSDataInputStream input = getFileSystem().open(file)) {
        return IOUtils.toByteArray(input);
      }
    });

    if (!MessageDigest.isEqual(referenceDigest, getDigest(content))) {
      throw new IllegalStateException(
          "Reference (=first) message digest doesn't match with digest of "
              + file.toString());
    }
  }
}
