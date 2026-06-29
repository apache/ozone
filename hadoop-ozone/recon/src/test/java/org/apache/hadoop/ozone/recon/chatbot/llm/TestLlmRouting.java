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

package org.apache.hadoop.ozone.recon.chatbot.llm;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link LlmRouting}. */
public class TestLlmRouting {

  private static final String DEFAULT_PROVIDER = "gemini";
  private static final String DEFAULT_MODEL = "gemini-2.5-flash";

  private LlmRouting routing;

  @BeforeEach
  public void setUp() {
    Map<String, List<String>> supportedModels = new HashMap<>();
    supportedModels.put("gemini", Arrays.asList("gemini-2.5-flash", "gemini-2.5-pro"));
    supportedModels.put("openai", Arrays.asList("gpt-4.1", "gpt-4.1-mini"));
    routing = new LlmRouting(DEFAULT_PROVIDER, DEFAULT_MODEL, supportedModels);
  }

  @Test
  public void testValidProviderAndModel() {
    LlmRouting.Resolved resolved = routing.resolve("openai", "gpt-4.1");
    assertEquals("openai", resolved.getProvider());
    assertEquals("gpt-4.1", resolved.getModel());
  }

  @Test
  public void testSupportedModelInfersProviderWhenProviderMissing() {
    LlmRouting.Resolved resolved = routing.resolve(null, "gpt-4.1");
    assertEquals("openai", resolved.getProvider());
    assertEquals("gpt-4.1", resolved.getModel());
  }

  @Test
  public void testUnsupportedModelFallsBackToDefaultModel() {
    LlmRouting.Resolved resolved = routing.resolve("gemini", "unknown-model");
    assertEquals("gemini", resolved.getProvider());
    assertEquals(DEFAULT_MODEL, resolved.getModel());
  }

  @Test
  public void testUnsupportedProviderInfersFromModel() {
    LlmRouting.Resolved resolved = routing.resolve("bad-provider", "gemini-2.5-pro");
    assertEquals("gemini", resolved.getProvider());
    assertEquals("gemini-2.5-pro", resolved.getModel());
  }

  @Test
  public void testUnsupportedProviderAndModelUseDefaults() {
    LlmRouting.Resolved resolved = routing.resolve("bad-provider", "unknown-model");
    assertEquals(DEFAULT_PROVIDER, resolved.getProvider());
    assertEquals(DEFAULT_MODEL, resolved.getModel());
  }

  @Test
  public void testMismatchedProviderAndModelResetToDefaults() {
    LlmRouting.Resolved resolved = routing.resolve("openai", "gemini-2.5-flash");
    assertEquals(DEFAULT_PROVIDER, resolved.getProvider());
    assertEquals(DEFAULT_MODEL, resolved.getModel());
  }
}
