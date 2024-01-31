/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.lease;

import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Test class to check functionality and consistency of LeaseManager.
 */
public class TestLeaseManager {

  /**
   * Dummy resource on which leases can be acquired.
   */
  private static final class DummyResource {

    private final String name;

    private DummyResource(String name) {
      this.name = name;
    }

    @Override
    public int hashCode() {
      return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof DummyResource) {
        return name.equals(((DummyResource) obj).name);
      }
      return false;
    }

    /**
     * Adding to String method to fix the ErrorProne warning that this method
     * is later used in String functions, which would print out (e.g.
     * `org.apache.hadoop.ozone.lease.TestLeaseManager.DummyResource@
     * 4488aabb`) instead of useful information.
     *
     * @return Name of the Dummy object.
     */
    @Override
    public String toString() {
      return "DummyResource{" +
          "name='" + name + '\'' +
          '}';
    }
  }

  @Test
  public void testLeaseAcquireAndRelease() throws LeaseException {
    //It is assumed that the test case execution won't take more than 5 seconds,
    //if it takes more time increase the defaultTimeout value of LeaseManager.
    LeaseManager<DummyResource> manager = new LeaseManager<>("Test", 5000);
    manager.start();
    DummyResource resourceOne = new DummyResource("one");
    DummyResource resourceTwo = new DummyResource("two");
    DummyResource resourceThree = new DummyResource("three");
    Lease<DummyResource> leaseOne = manager.acquire(resourceOne);
    Lease<DummyResource> leaseTwo = manager.acquire(resourceTwo);
    Lease<DummyResource> leaseThree = manager.acquire(resourceThree);
    assertEquals(leaseOne, manager.get(resourceOne));
    assertEquals(leaseTwo, manager.get(resourceTwo));
    assertEquals(leaseThree, manager.get(resourceThree));
    assertFalse(leaseOne.hasExpired());
    assertFalse(leaseTwo.hasExpired());
    assertFalse(leaseThree.hasExpired());
    //The below releases should not throw LeaseNotFoundException.
    manager.release(resourceOne);
    manager.release(resourceTwo);
    manager.release(resourceThree);
    assertTrue(leaseOne.hasExpired());
    assertTrue(leaseTwo.hasExpired());
    assertTrue(leaseThree.hasExpired());
    manager.shutdown();
  }

  @Test
  public void testLeaseAlreadyExist() throws LeaseException {
    LeaseManager<DummyResource> manager = new LeaseManager<>("Test", 5000);
    manager.start();
    DummyResource resourceOne = new DummyResource("one");
    DummyResource resourceTwo = new DummyResource("two");
    Lease<DummyResource> leaseOne = manager.acquire(resourceOne);
    Lease<DummyResource> leaseTwo = manager.acquire(resourceTwo);
    assertEquals(leaseOne, manager.get(resourceOne));
    assertEquals(leaseTwo, manager.get(resourceTwo));

    assertThrowsExactly(LeaseAlreadyExistException.class,
        () -> manager.acquire(resourceOne), "Resource: " + resourceOne);

    manager.release(resourceOne);
    manager.release(resourceTwo);
    manager.shutdown();
  }

  @Test
  public void testLeaseNotFound() throws LeaseException, InterruptedException {
    LeaseManager<DummyResource> manager = new LeaseManager<>("Test", 5000);
    manager.start();
    DummyResource resourceOne = new DummyResource("one");
    DummyResource resourceTwo = new DummyResource("two");
    DummyResource resourceThree = new DummyResource("three");

    //Case 1: lease was never acquired.
    assertThrowsExactly(LeaseNotFoundException.class,
        () -> manager.get(resourceOne), "Resource: " + resourceOne);

    //Case 2: lease is acquired and released.
    Lease<DummyResource> leaseTwo = manager.acquire(resourceTwo);
    assertEquals(leaseTwo, manager.get(resourceTwo));
    assertFalse(leaseTwo.hasExpired());
    manager.release(resourceTwo);
    assertTrue(leaseTwo.hasExpired());
    assertThrowsExactly(LeaseNotFoundException.class,
        () -> manager.get(resourceTwo), "Resource: " + resourceTwo);

    //Case 3: lease acquired and timed out.
    Lease<DummyResource> leaseThree = manager.acquire(resourceThree);
    assertEquals(leaseThree, manager.get(resourceThree));
    assertFalse(leaseThree.hasExpired());
    long sleepTime = leaseThree.getRemainingTime() + 1000;
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException ex) {
      //even in case of interrupt we have to wait till lease times out.
      Thread.sleep(sleepTime);
    }
    assertTrue(leaseThree.hasExpired());
    assertThrowsExactly(LeaseNotFoundException.class,
        () -> manager.get(resourceThree), "Resource: " + resourceThree);
    manager.shutdown();
  }

  @Test
  public void testCustomLeaseTimeout() throws LeaseException {
    LeaseManager<DummyResource> manager = new LeaseManager<>("Test", 5000);
    manager.start();
    DummyResource resourceOne = new DummyResource("one");
    DummyResource resourceTwo = new DummyResource("two");
    DummyResource resourceThree = new DummyResource("three");
    Lease<DummyResource> leaseOne = manager.acquire(resourceOne);
    Lease<DummyResource> leaseTwo = manager.acquire(resourceTwo, 10000);
    Lease<DummyResource> leaseThree = manager.acquire(resourceThree, 50000);
    assertEquals(leaseOne, manager.get(resourceOne));
    assertEquals(leaseTwo, manager.get(resourceTwo));
    assertEquals(leaseThree, manager.get(resourceThree));
    assertFalse(leaseOne.hasExpired());
    assertFalse(leaseTwo.hasExpired());
    assertFalse(leaseThree.hasExpired());
    assertEquals(5000, leaseOne.getLeaseLifeTime());
    assertEquals(10000, leaseTwo.getLeaseLifeTime());
    assertEquals(50000, leaseThree.getLeaseLifeTime());
    // Releasing of leases is done in shutdown, so don't have to worry about
    // lease release
    manager.shutdown();
  }

  @Test
  public void testLeaseCallback() throws LeaseException, InterruptedException {
    Map<DummyResource, String> leaseStatus = new HashMap<>();
    LeaseManager<DummyResource> manager = new LeaseManager<>("Test", 5000);
    manager.start();
    DummyResource resourceOne = new DummyResource("one");
    Lease<DummyResource> leaseOne = manager.acquire(resourceOne, () -> {
      leaseStatus.put(resourceOne, "lease expired");
      return null;
    });
    leaseStatus.put(resourceOne, "lease in use");
    // wait for lease to expire
    long sleepTime = leaseOne.getRemainingTime() + 1000;
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException ex) {
      //even in case of interrupt we have to wait till lease times out.
      Thread.sleep(sleepTime);
    }
    assertTrue(leaseOne.hasExpired());
    assertThrowsExactly(LeaseNotFoundException.class,
        () -> manager.get(resourceOne), "Resource: " + resourceOne);
    // check if callback has been executed
    assertEquals("lease expired", leaseStatus.get(resourceOne));
  }

  @Test
  public void testCallbackExecutionInCaseOfLeaseRelease()
      throws LeaseException, InterruptedException {
    // Callbacks should not be executed in case of lease release
    Map<DummyResource, String> leaseStatus = new HashMap<>();
    LeaseManager<DummyResource> manager = new LeaseManager<>("Test", 5000);
    manager.start();
    DummyResource resourceOne = new DummyResource("one");
    Lease<DummyResource> leaseOne = manager.acquire(resourceOne, () -> {
      leaseStatus.put(resourceOne, "lease expired");
      return null;
    });
    leaseStatus.put(resourceOne, "lease in use");
    leaseStatus.put(resourceOne, "lease released");
    manager.release(resourceOne);
    assertTrue(leaseOne.hasExpired());
    assertThrowsExactly(LeaseNotFoundException.class,
        () -> manager.get(resourceOne), "Resource: " + resourceOne);
    assertEquals("lease released", leaseStatus.get(resourceOne));
  }

  @Test
  public void testLeaseCallbackWithMultipleLeases()
      throws LeaseException, InterruptedException {
    Map<DummyResource, String> leaseStatus = new HashMap<>();
    LeaseManager<DummyResource> manager = new LeaseManager<>("Test", 5000);
    manager.start();
    DummyResource resourceOne = new DummyResource("one");
    DummyResource resourceTwo = new DummyResource("two");
    DummyResource resourceThree = new DummyResource("three");
    DummyResource resourceFour = new DummyResource("four");
    DummyResource resourceFive = new DummyResource("five");
    Lease<DummyResource> leaseOne = manager.acquire(resourceOne, () -> {
      leaseStatus.put(resourceOne, "lease expired");
      return null;
    });
    Lease<DummyResource> leaseTwo = manager.acquire(resourceTwo, () -> {
      leaseStatus.put(resourceTwo, "lease expired");
      return null;
    });
    Lease<DummyResource> leaseThree = manager.acquire(resourceThree, () -> {
      leaseStatus.put(resourceThree, "lease expired");
      return null;
    });
    Lease<DummyResource> leaseFour = manager.acquire(resourceFour, () -> {
      leaseStatus.put(resourceFour, "lease expired");
      return null;
    });
    Lease<DummyResource> leaseFive = manager.acquire(resourceFive, () -> {
      leaseStatus.put(resourceFive, "lease expired");
      return null;
    });
    leaseStatus.put(resourceOne, "lease in use");
    leaseStatus.put(resourceTwo, "lease in use");
    leaseStatus.put(resourceThree, "lease in use");
    leaseStatus.put(resourceFour, "lease in use");
    leaseStatus.put(resourceFive, "lease in use");

    // release lease one, two and three
    leaseStatus.put(resourceOne, "lease released");
    manager.release(resourceOne);
    leaseStatus.put(resourceTwo, "lease released");
    manager.release(resourceTwo);
    leaseStatus.put(resourceThree, "lease released");
    manager.release(resourceThree);

    // wait for other leases to expire
    long sleepTime = leaseFive.getRemainingTime() + 1000;

    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException ex) {
      //even in case of interrupt we have to wait till lease times out.
      Thread.sleep(sleepTime);
    }
    assertTrue(leaseOne.hasExpired());
    assertTrue(leaseTwo.hasExpired());
    assertTrue(leaseThree.hasExpired());
    assertTrue(leaseFour.hasExpired());
    assertTrue(leaseFive.hasExpired());

    assertEquals("lease released", leaseStatus.get(resourceOne));
    assertEquals("lease released", leaseStatus.get(resourceTwo));
    assertEquals("lease released", leaseStatus.get(resourceThree));
    assertEquals("lease expired", leaseStatus.get(resourceFour));
    assertEquals("lease expired", leaseStatus.get(resourceFive));
    manager.shutdown();
  }

  @Test
  public void testReuseReleasedLease() throws LeaseException {
    LeaseManager<DummyResource> manager = new LeaseManager<>("Test", 5000);
    manager.start();
    DummyResource resourceOne = new DummyResource("one");
    Lease<DummyResource> leaseOne = manager.acquire(resourceOne);
    assertEquals(leaseOne, manager.get(resourceOne));
    assertFalse(leaseOne.hasExpired());

    manager.release(resourceOne);
    assertTrue(leaseOne.hasExpired());

    Lease<DummyResource> sameResourceLease = manager.acquire(resourceOne);
    assertEquals(sameResourceLease, manager.get(resourceOne));
    assertFalse(sameResourceLease.hasExpired());

    manager.release(resourceOne);
    assertTrue(sameResourceLease.hasExpired());
    manager.shutdown();
  }

  @Test
  public void testReuseTimedOutLease()
      throws LeaseException, InterruptedException {
    LeaseManager<DummyResource> manager = new LeaseManager<>("Test", 5000);
    manager.start();
    DummyResource resourceOne = new DummyResource("one");
    Lease<DummyResource> leaseOne = manager.acquire(resourceOne);
    assertEquals(leaseOne, manager.get(resourceOne));
    assertFalse(leaseOne.hasExpired());
    // wait for lease to expire
    long sleepTime = leaseOne.getRemainingTime() + 1000;
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException ex) {
      //even in case of interrupt we have to wait till lease times out.
      Thread.sleep(sleepTime);
    }
    assertTrue(leaseOne.hasExpired());

    Lease<DummyResource> sameResourceLease = manager.acquire(resourceOne);
    assertEquals(sameResourceLease, manager.get(resourceOne));
    assertFalse(sameResourceLease.hasExpired());

    manager.release(resourceOne);
    assertTrue(sameResourceLease.hasExpired());
    manager.shutdown();
  }

  @Test
  public void testRenewLease() throws LeaseException, InterruptedException {
    LeaseManager<DummyResource> manager = new LeaseManager<>("Test", 5000);
    manager.start();
    DummyResource resourceOne = new DummyResource("one");
    Lease<DummyResource> leaseOne = manager.acquire(resourceOne);
    assertEquals(leaseOne, manager.get(resourceOne));
    assertFalse(leaseOne.hasExpired());

    // add 5 more seconds to the lease
    leaseOne.renew(5000);

    Thread.sleep(5000);

    // lease should still be active
    assertEquals(leaseOne, manager.get(resourceOne));
    assertFalse(leaseOne.hasExpired());
    manager.release(resourceOne);
    manager.shutdown();
  }

}
