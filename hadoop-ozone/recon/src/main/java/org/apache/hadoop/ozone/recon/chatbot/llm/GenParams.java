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

/**
 * Immutable generation settings for a single LLM call.
 *
 * <p>Replaces the previous untyped {@code Map<String, Object>} parameter bag. Both values
 * are always supplied by callers, so they are primitives rather than nullable boxes.
 * LangChain4j 0.35.0 does not support per-request overrides on {@code ChatRequest}, so these
 * are applied when the provider model is built (and are part of the model cache key).</p>
 */
public final class GenParams {

  private final double temperature;
  private final int maxTokens;

  public GenParams(double temperature, int maxTokens) {
    this.temperature = temperature;
    this.maxTokens = maxTokens;
  }

  public double temperature() {
    return temperature;
  }

  public int maxTokens() {
    return maxTokens;
  }
}
