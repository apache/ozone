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

package org.apache.hadoop.ozone.shell;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.base.Strings;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneDatanodeShell.class);

  private static HddsDatanodeService datanode = null;

  /**
   * Create a HddsDatanodeService stub that can be used to test Cli behavior.
   *
   * @throws Exception
   */
  @BeforeAll
  public static void init() {
    datanode = new TestHddsDatanodeService(new String[] {});
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
      Exception ex = assertThrows(Exception.class, () -> executeDatanode(hdds, args));
      if (!Strings.isNullOrEmpty(expectedError)) {
        Throwable exceptionToCheck = ex;
        if (exceptionToCheck.getCause() != null) {
          exceptionToCheck = exceptionToCheck.getCause();
        }
        assertThat(exceptionToCheck.getMessage()).contains(expectedError);
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
    TestHddsDatanodeService(String[] args) {
      super(args);
    }

    @Override
    public void start() {
      // do nothing
    }
  }
}
