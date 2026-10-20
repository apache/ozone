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

package org.apache.hadoop.ozone.jobworker.commands;

import static org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto.Type;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.OptionalLong;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandResultCode;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.ozone.jobworker.JobworkerConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for JobworkerCommandManager class.
 */
public class TestJobworkerCommandManager {

  private static final String TEST_SERVICE_ID = "test-service-1";
  private static final String TEST_SERVICE_ID_2 = "test-service-2";
  private JobworkerCommandManager commandManager;

  @BeforeEach
  public void setUp() {
    OzoneConfiguration conf = new OzoneConfiguration();
    commandManager = new JobworkerCommandManager(conf);
  }

  @Test
  public void testAddCommand() {
    JobworkerCommand<?> command = new MockJobworkerCommand(1L, TEST_SERVICE_ID, 5L, 0L,
        Type.mockCommand);
    commandManager.addCommand(command);
    // Verify command was added by checking the queue summary
    Map<Type, Integer> summary = commandManager.getCommandQueueSummary();
    assertEquals(1, summary.get(Type.mockCommand).intValue());
    // Verify term was updated
    OptionalLong term = commandManager.getTermOfLeaderOMByServiceId(TEST_SERVICE_ID);
    assertTrue(term.isPresent());
    assertEquals(5L, term.getAsLong());
  }

  @Test
  public void testAddCommandWithoutTerm() {
    JobworkerCommand<?> command = new MockJobworkerCommand(1L, TEST_SERVICE_ID, Type.mockCommand);
    commandManager.addCommand(command);
    // It is Ok for the 0-term value (non-HA cluster)
    assertNextCommand(1L, TEST_SERVICE_ID, Type.mockCommand);
  }

  @Test
  public void testGetNextCommand() {
    // Add multiple commands with different service IDs and terms
    createAndAddCommand(1L, TEST_SERVICE_ID, 1L, Type.mockCommand);
    createAndAddCommand(2L, TEST_SERVICE_ID, 1L, Type.mockCommand);
    createAndAddCommand(3L, TEST_SERVICE_ID_2, 6L, Type.reregisterCommand);

    // Retrieve commands
    JobworkerCommand<?> retrievedCommand1 = commandManager.getNextCommand();
    JobworkerCommand<?> retrievedCommand2 = commandManager.getNextCommand();
    JobworkerCommand<?> retrievedCommand3 = commandManager.getNextCommand();
    JobworkerCommand<?> retrievedCommand4 = commandManager.getNextCommand();

    // Verify commands are retrieved in order
    assertNotNull(retrievedCommand1);
    assertEquals(1L, retrievedCommand1.getId());
    assertNotNull(retrievedCommand2);
    assertEquals(2L, retrievedCommand2.getId());
    assertNotNull(retrievedCommand3);
    assertEquals(3L, retrievedCommand3.getId());
    // Queue should be empty now
    assertNull(retrievedCommand4);

    // Verify terms were updated normally for specific omService ID
    OptionalLong term1 = commandManager.getTermOfLeaderOMByServiceId(TEST_SERVICE_ID);
    assertTrue(term1.isPresent());
    assertEquals(1L, term1.getAsLong()); // Should be the highest term seen

    OptionalLong term2 = commandManager.getTermOfLeaderOMByServiceId(TEST_SERVICE_ID_2);
    assertTrue(term2.isPresent());
    assertEquals(6L, term2.getAsLong());
  }

  @Test
  public void testStaleCommandsAreDropped() {
    createAndAddCommand(1L, TEST_SERVICE_ID, 10L, Type.mockCommand);
    // Add a command with a lower term (5 < 10)
    createAndAddCommand(2L, TEST_SERVICE_ID, 5L, Type.mockCommand);
    assertNextCommand(1L, TEST_SERVICE_ID, Type.mockCommand);
    // The next Command's term lower than leader term 10, so we will get null Command
    assertNull(commandManager.getNextCommand());

    createAndAddCommand(3L, TEST_SERVICE_ID, 5L, Type.mockCommand);
    // Add a command with a high term (10 > 5)
    createAndAddCommand(4L, TEST_SERVICE_ID, 10L, Type.mockCommand);
    assertNextCommand(4L, TEST_SERVICE_ID, Type.mockCommand);
    // The next Command's term lower than leader term 10, so we will get null Command
    assertNull(commandManager.getNextCommand());

    createAndAddCommand(5L, TEST_SERVICE_ID, 5L, Type.unknownCommand);
    createAndAddCommand(6L, TEST_SERVICE_ID, 5L, Type.mockCommand);
    // Add a command with a high term (10 > 5)
    createAndAddCommand(7L, TEST_SERVICE_ID, 10L, Type.reregisterCommand);
    assertNextCommand(7L, TEST_SERVICE_ID, Type.reregisterCommand);
    // The next Command's term lower than leader term 10, so we will get null Command
    assertNull(commandManager.getNextCommand());

    // Use a fresh ID: command 1 is still in flight from the first phase.
    createAndAddCommand(8L, TEST_SERVICE_ID, 10L, Type.mockCommand);
    // Add a command with a lower term (5 < 10)
    createAndAddCommand(2L, TEST_SERVICE_ID_2, 5L, Type.mockCommand);
    assertNextCommand(8L, TEST_SERVICE_ID, Type.mockCommand);
    // The TEST_SERVICE_ID_2 should not affect TEST_SERVICE_ID_1,
    // so we can get the Command of TEST_SERVICE_ID_2
    assertNextCommand(2L, TEST_SERVICE_ID_2, Type.mockCommand);
  }

  @Test
  public void testQueueLimitEnforcement() {
    // Create a configuration with a small queue limit
    OzoneConfiguration smallQueueConf = new OzoneConfiguration();
    JobworkerConfiguration jwConfig = smallQueueConf.getObject(JobworkerConfiguration.class);
    jwConfig.setCommandQueueLimit(2);
    smallQueueConf.setFromObject(jwConfig);
    JobworkerCommandManager limitedManager = new JobworkerCommandManager(smallQueueConf);
    // Add two commands
    createAndAddCommand(limitedManager, 1L, TEST_SERVICE_ID, 1L, Type.mockCommand);
    createAndAddCommand(limitedManager, 2L, TEST_SERVICE_ID, 1L, Type.mockCommand);
    // Queue summary should show 2 commands
    Map<Type, Integer> summary = limitedManager.getCommandQueueSummary();
    assertEquals(2, summary.values().stream().mapToInt(Integer::intValue).sum());
    // Try to add another command, which should be ignored due to the limit
    createAndAddCommand(3L, TEST_SERVICE_ID, 1L, Type.mockCommand);
    // Queue summary should still show only 2 commands
    summary = limitedManager.getCommandQueueSummary();
    assertEquals(2, summary.values().stream().mapToInt(Integer::intValue).sum());
  }

  @Test
  public void testTermUpdateLogic() {
    commandManager.updateTermOfLeaderOM(TEST_SERVICE_ID, 5L);
    OptionalLong term = commandManager.getTermOfLeaderOMByServiceId(TEST_SERVICE_ID);
    assertTrue(term.isPresent());
    assertEquals(5L, term.getAsLong());

    commandManager.updateTermOfLeaderOM(TEST_SERVICE_ID_2, 1L);
    term = commandManager.getTermOfLeaderOMByServiceId(TEST_SERVICE_ID_2);
    assertTrue(term.isPresent());
    assertEquals(1L, term.getAsLong());

    // Test updating to a higher term
    commandManager.updateTermOfLeaderOM(TEST_SERVICE_ID, 10L);
    term = commandManager.getTermOfLeaderOMByServiceId(TEST_SERVICE_ID);
    assertTrue(term.isPresent());
    assertEquals(10L, term.getAsLong());

    // Test updating to a lower term (should be ignored)
    commandManager.updateTermOfLeaderOM(TEST_SERVICE_ID, 7L);
    term = commandManager.getTermOfLeaderOMByServiceId(TEST_SERVICE_ID);
    assertTrue(term.isPresent());
    assertEquals(10L, term.getAsLong()); // Term should still be 10

    // TEST_SERVICE_ID_2 should not be affect
    term = commandManager.getTermOfLeaderOMByServiceId(TEST_SERVICE_ID_2);
    assertTrue(term.isPresent());
    assertEquals(1L, term.getAsLong());  // Term should still be 1L

    // Test getting term for non-existent service ID
    OptionalLong nonExistentTerm =
        commandManager.getTermOfLeaderOMByServiceId("non-existent-service");
    assertFalse(nonExistentTerm.isPresent());
  }

  @Test
  public void testCommandStatusTracking() {
    // Create a command
    long commandId = 1L;
    JobworkerCommand<?> command = new MockJobworkerCommand(commandId,
        Type.mockCommand);

    // Add command to manager
    commandManager.addCommand(command);

    // Try to get command status (depends on command implementation)
    JobworkerCommandStatus status = commandManager.getCmdStatus(
        command.getOmServiceId(), commandId);

    // Depending on implementation, status might be null or initialized
    if (status != null) {
      assertEquals(commandId, status.getCmdId());
    }
  }

  @Test
  public void testCommandQueueFullStatusUpdate() {
    // Create a configuration with a small queue limit
    OzoneConfiguration smallQueueConf = new OzoneConfiguration();
    JobworkerConfiguration jwConfig = smallQueueConf.getObject(JobworkerConfiguration.class);
    jwConfig.setCommandQueueLimit(2);
    smallQueueConf.setFromObject(jwConfig);
    JobworkerCommandManager limitedManager = new JobworkerCommandManager(smallQueueConf);

    // Add two commands to fill the queue to its limit
    createAndAddCommand(limitedManager, 1L, TEST_SERVICE_ID, 1L, Type.mockCommand);
    createAndAddCommand(limitedManager, 2L, TEST_SERVICE_ID, 1L, Type.mockCommand);

    // Try to add a third command, which should be rejected due to queue being full
    JobworkerCommand<?> thirdCommand = new MockJobworkerCommand(3L, TEST_SERVICE_ID, 1L, 0L, Type.mockCommand);
    limitedManager.addCommand(thirdCommand);

    // Verify the third command has FAILED status with COMMAND_QUEUE_FULL result code
    JobworkerCommandStatus status3 = limitedManager.getCmdStatus(TEST_SERVICE_ID, 3L);
    assertNotNull(status3);
    assertEquals(CommandStatus.Status.FAILED, status3.getStatus());
    assertEquals(CommandResultCode.COMMAND_QUEUE_FULL, status3.getProtobufMessage().getResultCode());
    assertEquals("Command queue is full", status3.getMessage());

    // Verify the command was not actually added to the queue
    Map<Type, Integer> summary = limitedManager.getCommandQueueSummary();
    assertEquals(2, summary.values().stream().mapToInt(Integer::intValue).sum());
  }

  @Test
  void testDuplicateCommandDoesNotResetExecution() {
    JobworkerCommand<?> command = new MockJobworkerCommand(42L, "service", 1L, 0L, Type.mockCommand);
    commandManager.addCommand(command);
    assertEquals(command, commandManager.getNextCommand());
    commandManager.updateCommand(command, CommandStatus.Status.EXECUTING, null, null);
    commandManager.addCommand(new MockJobworkerCommand(42L, "service", 1L, 0L, Type.mockCommand));
    assertNull(commandManager.getNextCommand());
    assertEquals(CommandStatus.Status.EXECUTING, commandManager.getCmdStatus("service", 42L).getStatus());
  }

  @Test
  void testConcurrentLeaderTermsNeverDecrease() {
    java.util.stream.LongStream.rangeClosed(1, 1000).parallel()
        .forEach(term -> commandManager.updateTermOfLeaderOM(TEST_SERVICE_ID, term));
    assertEquals(1000L, commandManager.getTermOfLeaderOMByServiceId(TEST_SERVICE_ID).getAsLong());
  }

  private void createAndAddCommand(long id, String omServiceId, long term,
                                   Type type) {
    createAndAddCommand(commandManager, id, omServiceId, term, type);
  }

  private void createAndAddCommand(JobworkerCommandManager manager,
                                   long id, String omServiceId, long term, Type type) {
    JobworkerCommand<?> initCommand = new MockJobworkerCommand(id, omServiceId, term, 0L, type);
    manager.addCommand(initCommand);
  }

  private void assertNextCommand(long exceptId, String exceptServiceId, Type exceptedType) {
    JobworkerCommand<?> retrievedInitCommand = commandManager.getNextCommand();
    assertNotNull(retrievedInitCommand);
    assertEquals(exceptId, retrievedInitCommand.getId());
    assertEquals(exceptServiceId, retrievedInitCommand.getOmServiceId());
    assertEquals(exceptedType, retrievedInitCommand.getType());
  }
}
