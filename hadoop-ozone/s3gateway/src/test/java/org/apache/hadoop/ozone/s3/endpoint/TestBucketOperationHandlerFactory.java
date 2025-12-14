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

package org.apache.hadoop.ozone.s3.endpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test class for BucketOperationHandlerFactory.
 */
public class TestBucketOperationHandlerFactory {

  private BucketOperationHandlerFactory factory;

  @BeforeEach
  public void setup() {
    factory = new BucketOperationHandlerFactory();
  }

  @Test
  public void testDefaultHandlersRegistered() {
    // Verify that the default ACL handler is registered
    assertTrue(factory.hasHandler("acl"),
        "ACL handler should be registered by default");
    assertNotNull(factory.getHandler("acl"),
        "ACL handler should not be null");
  }

  @Test
  public void testGetHandlerForAcl() {
    BucketOperationHandler handler = factory.getHandler("acl");
    assertNotNull(handler, "ACL handler should exist");
    assertTrue(handler instanceof AclHandler,
        "Handler should be an instance of AclHandler");
    assertEquals("acl", handler.getQueryParamName(),
        "Handler query param name should be 'acl'");
  }

  @Test
  public void testGetHandlerForNonExistentParam() {
    BucketOperationHandler handler = factory.getHandler("nonexistent");
    assertNull(handler, "Handler for non-existent param should be null");
  }

  @Test
  public void testHasHandlerReturnsTrueForExisting() {
    assertTrue(factory.hasHandler("acl"),
        "Should return true for existing handler");
  }

  @Test
  public void testHasHandlerReturnsFalseForNonExisting() {
    assertFalse(factory.hasHandler("nonexistent"),
        "Should return false for non-existing handler");
  }

  @Test
  public void testRegisterNewHandler() {
    // Create a mock handler
    BucketOperationHandler mockHandler = new MockBucketOperationHandler("test");

    // Register the handler
    factory.register(mockHandler);

    // Verify registration
    assertTrue(factory.hasHandler("test"),
        "Newly registered handler should exist");
    assertEquals(mockHandler, factory.getHandler("test"),
        "Retrieved handler should be the same instance");
  }

  @Test
  public void testRegisterOverwritesExistingHandler() {
    // Register a new handler with the same query param as ACL
    BucketOperationHandler mockHandler = new MockBucketOperationHandler("acl");

    factory.register(mockHandler);

    // Verify the handler was overwritten
    BucketOperationHandler handler = factory.getHandler("acl");
    assertEquals(mockHandler, handler,
        "Handler should be the newly registered one");
    assertTrue(handler instanceof MockBucketOperationHandler,
        "Handler should be an instance of MockBucketOperationHandler");
  }

  @Test
  public void testGetRegisteredQueryParams() {
    // Default should have at least "acl"
    assertTrue(factory.getRegisteredQueryParams().contains("acl"),
        "Registered query params should contain 'acl'");

    // Register additional handlers
    factory.register(new MockBucketOperationHandler("lifecycle"));
    factory.register(new MockBucketOperationHandler("notification"));

    // Verify all are present
    assertEquals(3, factory.getRegisteredQueryParams().size(),
        "Should have 3 registered handlers");
    assertTrue(factory.getRegisteredQueryParams().contains("lifecycle"),
        "Should contain 'lifecycle'");
    assertTrue(factory.getRegisteredQueryParams().contains("notification"),
        "Should contain 'notification'");
  }

  @Test
  public void testMultipleHandlerRegistration() {
    BucketOperationHandler handler1 = new MockBucketOperationHandler("test1");
    BucketOperationHandler handler2 = new MockBucketOperationHandler("test2");
    BucketOperationHandler handler3 = new MockBucketOperationHandler("test3");

    factory.register(handler1);
    factory.register(handler2);
    factory.register(handler3);

    assertTrue(factory.hasHandler("test1"), "Handler test1 should exist");
    assertTrue(factory.hasHandler("test2"), "Handler test2 should exist");
    assertTrue(factory.hasHandler("test3"), "Handler test3 should exist");

    assertEquals(handler1, factory.getHandler("test1"));
    assertEquals(handler2, factory.getHandler("test2"));
    assertEquals(handler3, factory.getHandler("test3"));
  }

  /**
   * Mock implementation of BucketOperationHandler for testing.
   */
  private static class MockBucketOperationHandler implements BucketOperationHandler {
    private final String queryParamName;

    MockBucketOperationHandler(String queryParamName) {
      this.queryParamName = queryParamName;
    }

    @Override
    public Response handlePutRequest(String bucketName, InputStream body,
                                     HttpHeaders headers,
                                     BucketEndpointContext context,
                                     long startNanos)
        throws IOException, OS3Exception {
      return Response.ok().build();
    }

    @Override
    public String getQueryParamName() {
      return queryParamName;
    }
  }
}
