/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.client.rpc;

import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.protocol.ClientId;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.when;

/**
 * Test cases for LeaseRenewer.
 */
public class TestLeaseRenewer {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestLeaseRenewer.class);
  private final String fakeAuthority = "hdfs://nn1/";
  private final UserGroupInformation fakeUgiA =
      UserGroupInformation.createUserForTesting(
          "myuser", new String[]{"group1"});
  private final UserGroupInformation fakeUgiB =
      UserGroupInformation.createUserForTesting(
          "myuser", new String[]{"group1"});

  private RpcClient mockRpcClient;
  private LeaseRenewer renewer;

  /** Cause renewals often so test runs quickly. */
  private static final long FAST_GRACE_PERIOD = 100L;

  @Before
  public void setupMocksAndRenewer() throws IOException {
    mockRpcClient = createMockClient();

    renewer = LeaseRenewer.getInstance(fakeAuthority, fakeUgiA, mockRpcClient);
    renewer.setGraceSleepPeriod(FAST_GRACE_PERIOD);
  }

  private RpcClient createMockClient() {
    final OzoneClientConfig mockConf = Mockito.mock(OzoneClientConfig.class);
    when(mockConf.getTimeout()).thenReturn((int)FAST_GRACE_PERIOD);

    RpcClient mock = Mockito.mock(RpcClient.class);
    when(mock.isClientRunning()).thenReturn(true);
    when(mock.getConf()).thenReturn(mockConf);

    ClientId clientId = ClientId.randomId();
    when(mock.getClientId()).thenReturn(clientId);
    return mock;
  }

  @Test
  public void testInstanceSharing() throws IOException {
    // Two lease renewers with the same UGI should return
    // the same instance
    LeaseRenewer lr =
        LeaseRenewer.getInstance(fakeAuthority, fakeUgiA, mockRpcClient);
    LeaseRenewer lr2 =
        LeaseRenewer.getInstance(fakeAuthority, fakeUgiA, mockRpcClient);
    Assert.assertSame(lr, lr2);

    // But a different UGI should return a different instance
    LeaseRenewer lr3 =
        LeaseRenewer.getInstance(fakeAuthority, fakeUgiB, mockRpcClient);
    Assert.assertNotSame(lr, lr3);

    // A different authority with same UGI should also be a different
    // instance.
    LeaseRenewer lr4 = LeaseRenewer.getInstance(
        "someOtherAuthority", fakeUgiB, mockRpcClient);
    Assert.assertNotSame(lr, lr4);
    Assert.assertNotSame(lr3, lr4);
  }

  @Test
  public void testRenewal() throws Exception {
    // Keep track of how many times the lease gets renewed
    final AtomicInteger leaseRenewalCount = new AtomicInteger();
    Mockito.doAnswer((Answer<Boolean>) invocation -> {
      leaseRenewalCount.incrementAndGet();
      return true;
    }).when(mockRpcClient).renewLease();


    // Set up a file so that we start renewing our lease.
    renewer.startRenewerDaemonIfApplicable(mockRpcClient);

    // Wait for lease to get renewed
    long failTime = Time.monotonicNow() + 5000;
    while (Time.monotonicNow() < failTime &&
        leaseRenewalCount.get() == 0) {
      Thread.sleep(50);
    }
    if (leaseRenewalCount.get() == 0) {
      Assert.fail("Did not renew lease at all!");
    }

    renewer.closeClient(mockRpcClient);
  }

  /**
   * Regression test for HDFS-2810. In this bug, the LeaseRenewer has handles
   * to several DFSClients with the same name, the first of which has no files
   * open. Previously, this was causing the lease to not get renewed.
   */
  @Test
  public void testManyDfsClientsWhereSomeNotOpen() throws Exception {
    // First DFSClient has no files open so doesn't renew leases.
    final RpcClient mockClient1 = createMockClient();
    Mockito.doReturn(false).when(mockClient1).renewLease();
    assertSame(renewer,
        LeaseRenewer.getInstance(fakeAuthority, fakeUgiA, mockClient1));
    long fileId = 456L;
    renewer.startRenewerDaemonIfApplicable(mockClient1);

    // Second DFSClient does renew lease
    final RpcClient mockClient2 = createMockClient();
    Mockito.doReturn(true).when(mockClient2).renewLease();
    assertSame(renewer,
        LeaseRenewer.getInstance(fakeAuthority, fakeUgiA, mockClient2));

    renewer.startRenewerDaemonIfApplicable(mockClient2);


    // Wait for lease to get renewed
    GenericTestUtils.waitFor(() -> {
      try {
        Mockito.verify(mockClient1, Mockito.atLeastOnce()).renewLease();
        Mockito.verify(mockClient2, Mockito.atLeastOnce()).renewLease();
        return true;
      } catch (AssertionError err) {
        LeaseRenewer.LOG.warn("Not yet satisfied", err);
        return false;
      } catch (IOException e) {
        // should not throw!
        throw new RuntimeException(e);
      }
    }, 100, 10000);

    renewer.closeClient(mockClient1);
    renewer.closeClient(mockClient2);
    renewer.closeClient(mockRpcClient);

    // Make sure renewer is not running due to expiration.
    Thread.sleep(FAST_GRACE_PERIOD * 2);
    Assert.assertTrue(!renewer.isRunning());
  }

  @Test
  public void testThreadName() throws Exception {
    Assert.assertFalse("Renewer not initially running",
        renewer.isRunning());

    // Pretend to open a file
    renewer.startRenewerDaemonIfApplicable(mockRpcClient);

    Assert.assertTrue("Renewer should have started running",
        renewer.isRunning());

    // Check the thread name is reasonable
    String threadName = renewer.getDaemonName();
    Assert.assertEquals("LeaseRenewer:myuser@hdfs://nn1/", threadName);

    // Pretend to close the file
    renewer.closeClient(mockRpcClient);
    renewer.setEmptyTime(Time.monotonicNow());

    // Should stop the renewer running within a few seconds
    long failTime = Time.monotonicNow() + 5000;
    while (renewer.isRunning() && Time.monotonicNow() < failTime) {
      Thread.sleep(50);
    }
    Assert.assertFalse(renewer.isRunning());
  }

  /**
   * Test for HDFS-14575. In this fix, the LeaseRenewer clears all clients
   * and expires immediately via setting empty time to 0 before it's removed
   * from factory. Previously, LeaseRenewer#daemon thread might leak.
   */
  @Test
  public void testDaemonThreadLeak() throws Exception {
    Assert.assertFalse("Renewer not initially running", renewer.isRunning());

    // Pretend to create a file#1, daemon#1 starts
    renewer.startRenewerDaemonIfApplicable(mockRpcClient);
    Assert.assertTrue("Renewer should have started running",
        renewer.isRunning());
    Pattern daemonThreadNamePattern = Pattern.compile("LeaseRenewer:\\S+");
    Assert.assertEquals(1, countThreadMatching(daemonThreadNamePattern));

    // Pretend to create file#2, daemon#2 starts due to expiration
    LeaseRenewer lastRenewer = renewer;
    renewer =
        LeaseRenewer.getInstance(fakeAuthority, fakeUgiA, mockRpcClient);
    Assert.assertEquals(lastRenewer, renewer);

    // Pretend to close file#1
    renewer.closeClient(mockRpcClient);
    Assert.assertEquals(1, countThreadMatching(daemonThreadNamePattern));

    // Pretend to be expired
    renewer.setEmptyTime(0);

    renewer =
        LeaseRenewer.getInstance(fakeAuthority, fakeUgiA, mockRpcClient);
    renewer.setGraceSleepPeriod(FAST_GRACE_PERIOD);
    boolean success = renewer.startRenewerDaemonIfApplicable(mockRpcClient);
    if (!success) {
      LeaseRenewer.remove(renewer);
      renewer =
          LeaseRenewer.getInstance(fakeAuthority, fakeUgiA, mockRpcClient);
      renewer.setGraceSleepPeriod(FAST_GRACE_PERIOD);
      renewer.startRenewerDaemonIfApplicable(mockRpcClient);
    }

    int threadCount = countThreadMatching(daemonThreadNamePattern);
    //Sometimes old LR#Daemon gets closed and lead to count 1 (rare scenario)
    Assert.assertTrue(1 == threadCount || 2 == threadCount);

    // After grace period, both daemon#1 and renewer#1 will be removed due to
    // expiration, then daemon#2 will leak before HDFS-14575.
    Thread.sleep(FAST_GRACE_PERIOD * 2);

    // Pretend to close file#2, renewer#2 will be created
    lastRenewer = renewer;
    renewer =
        LeaseRenewer.getInstance(fakeAuthority, fakeUgiA, mockRpcClient);
    Assert.assertEquals(lastRenewer, renewer);
    renewer.setGraceSleepPeriod(FAST_GRACE_PERIOD);
    renewer.closeClient(mockRpcClient);
    renewer.setEmptyTime(0);
    // Make sure LeaseRenewer#daemon threads will terminate after grace period
    Thread.sleep(FAST_GRACE_PERIOD * 2);
    Assert.assertEquals("LeaseRenewer#daemon thread leaks", 0,
        countThreadMatching(daemonThreadNamePattern));
  }

  private static int countThreadMatching(Pattern pattern) {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] infos =
        threadBean.getThreadInfo(threadBean.getAllThreadIds(), 1);
    int count = 0;
    for (ThreadInfo info : infos) {
      if (info == null) {
        continue;
      }
      if (pattern.matcher(info.getThreadName()).matches()) {
        LOG.info(info.getThreadName() + " matches pattern " + pattern);
        count++;
      }
    }
    return count;
  }
}
