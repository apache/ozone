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

import java.util.concurrent.Callable;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Directory Generator tool to test OM performance.
 */
@Command(name = "ddsg",
    aliases = "dfs-directory-generator",
    description =
        "Create nested directories to the any dfs compatible file system.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
@SuppressWarnings("java:S2245") // no need for secure random
public class HadoopNestedDirGenerator extends HadoopBaseFreonGenerator
    implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(HadoopNestedDirGenerator.class);

  @Option(names = {"-d", "--depth"},
      description = "Number of directories to be generated recursively",
      defaultValue = "5")
  private int depth;

  @Option(names = {"-s", "--span"},
      description =
          "Number of child directories to be created in leaf directory.",
      defaultValue = "10")
  private int span;

  @Option(names = {"-l", "--name-len", "--nameLen"},
      description =
          "Length of the random name of directory you want to create. Full " +
              "name --nameLen will be removed in later versions.",
      defaultValue = "10")
  private int length;

  @Override
  public Void call() throws Exception {
    String s;
    if (depth <= 0) {
      s = "Invalid depth value, depth value should be greater than zero!";
      print(s);
    } else if (span < 0) {
      s = "Invalid span value, span value should be greater or equal to zero!";
      print(s);
    } else {
      super.init();
      runTests(this::createDir);
    }
    return null;
  }

  /*
      Nested directories will be created like this,
      suppose you pass depth=3, span=3 and number of tests=2

              Dir11                               Dir12
                |                                   |
              Dir21                               Dir22
                |                                   |
              Dir31                               Dir32
            /   |   \                           /   |   \
      Dir311 Dir312 Dir313                 Dir321 Dir322 Dir323

   */
  private void createDir(long counter) throws Exception {
    String dirString = RandomStringUtils.secure().nextAlphanumeric(length);
    for (int i = 1; i <= depth; i++) {
      dirString = dirString.concat("/").concat(RandomStringUtils.
          secure().nextAlphanumeric(length));
    }
    Path file = new Path(getRootPath().concat("/").concat(dirString));
    getFileSystem().mkdirs(file.getParent());
    String leafDir = dirString.substring(0, dirString.length() - length);
    String tmp = "/0";
    for (int i = 1; i <= span; i++) {
      String childDir = leafDir.concat(Integer.toString(i)).concat(tmp);
      Path dir = new Path(getRootPath().concat("/").concat(childDir));
      getFileSystem().mkdirs(dir.getParent());
    }
    String message = "\nSuccessfully created directories. " +
            "Total Directories with level = " + depth + " and span = " + span;
    print(message);
  }
}
