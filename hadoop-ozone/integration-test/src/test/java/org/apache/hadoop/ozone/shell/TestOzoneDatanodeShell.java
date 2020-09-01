/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.shell;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.ozone.HddsDatanodeService;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import org.junit.Rule;
import org.junit.rules.Timeout;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.IExceptionHandler2;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunLast;

/**
 * This test class specified for testing Ozone datanode shell command.
 */
public class TestOzoneDatanodeShell {

  /**
    * Set a timeout for each test.
    */
  @Rule
  public Timeout timeout = new Timeout(300000);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneDatanodeShell.class);

  private static HddsDatanodeService datanode = null;

  /**
   * Create a HddsDatanodeService stub that can be used to test Cli behavior.
   *
   * @throws Exception
   */
  @BeforeClass
  public static void init() {
    datanode = new TestHddsDatanodeService(false, new String[] {});
  }
  
  private void executeDatanode(HddsDatanodeService hdds, String[] args) {
    LOG.info("Executing datanode command with args {}", Arrays.asList(args));
    CommandLine cmd = hdds.getCmd();

    IExceptionHandler2<List<Object>> exceptionHandler =
        new IExceptionHandler2<List<Object>>() {
          @Override
          public List<Object> handleParseException(ParameterException ex,
              String[] args) {
            throw ex;
          }

          @Override
          public List<Object> handleExecutionException(ExecutionException ex,
              ParseResult parseResult) {
            throw ex;
          }
        };
    cmd.parseWithHandlers(new RunLast(),
        exceptionHandler, args);
  }

  /**
   * Execute command, assert exception message and returns true if error
   * was thrown and contains the specified usage string.
   */
  private void executeDatanodeWithError(HddsDatanodeService hdds, String[] args,
      String expectedError) {
    if (Strings.isNullOrEmpty(expectedError)) {
      executeDatanode(hdds, args);
    } else {
      try {
        executeDatanode(hdds, args);
        fail("Exception is expected from command execution " + Arrays
            .asList(args));
      } catch (Exception ex) {
        if (!Strings.isNullOrEmpty(expectedError)) {
          Throwable exceptionToCheck = ex;
          if (exceptionToCheck.getCause() != null) {
            exceptionToCheck = exceptionToCheck.getCause();
          }
          Assert.assertTrue(
              String.format(
                  "Error of shell code doesn't contain the " +
                      "exception [%s] in [%s]",
                  expectedError, exceptionToCheck.getMessage()),
              exceptionToCheck.getMessage().contains(expectedError));
        }
      }
    }
  }

  @Test
  public void testDatanodeCommand() {
    LOG.info("Running testDatanodeIncompleteCommand");
    String[] args = new String[]{}; //executing 'ozone datanode'

    //'ozone datanode' command should not result in error
    executeDatanodeWithError(datanode, args, null);
  }

  @Test
  public void testDatanodeInvalidParamCommand() {
    LOG.info("Running testDatanodeIncompleteCommand");
    String expectedError = "Unknown option: '-invalidParam'";
    //executing 'ozone datanode -invalidParam'
    String[] args = new String[]{"-invalidParam"};

    executeDatanodeWithError(datanode, args, expectedError);
  }

  private static class TestHddsDatanodeService extends HddsDatanodeService {
    TestHddsDatanodeService(boolean printBanner, String[] args) {
      super(printBanner, args);
    }

    @Override
    public void start() {
      // do nothing
    }
  }
}
