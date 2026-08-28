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

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * Resolves user-requested provider/model into an effective pair using configured defaults.
 *
 * <p>Rules:
 * <ol>
 *   <li>Provider: use user value if configured with an API key; else infer from supported model;
 *       else use default provider.</li>
 *   <li>Model: use user value if present in any configured model list; else use default model.</li>
 *   <li>If the model is not listed for the chosen provider, reset to default provider + default model.</li>
 * </ol>
 */
final class LlmRouting {

  private final String defaultProvider;
  private final String defaultModel;
  private final Map<String, List<String>> supportedModels;

  LlmRouting(String defaultProvider, String defaultModel,
             Map<String, List<String>> supportedModels) {
    this.defaultProvider = defaultProvider;
    this.defaultModel = defaultModel;
    this.supportedModels = supportedModels;
  }

  Resolved resolve(String userProvider, String userModel) {
    String requestedProvider = normalizeProvider(userProvider);
    String requestedModel = normalizeModel(userModel);

    String effectiveProvider;
    if (isSupportedProvider(requestedProvider)) {
      effectiveProvider = requestedProvider;
    } else {
      String inferred = findProviderForModel(requestedModel);
      effectiveProvider = inferred != null ? inferred : defaultProvider;
    }

    String effectiveModel = isSupportedModel(requestedModel) ? requestedModel : defaultModel;

    List<String> providerModels = supportedModels.get(effectiveProvider);
    if (providerModels == null || !providerModels.contains(effectiveModel)) {
      effectiveProvider = defaultProvider;
      effectiveModel = defaultModel;
    }

    return new Resolved(effectiveProvider, effectiveModel);
  }

  private static String normalizeProvider(String value) {
    if (StringUtils.isBlank(value)) {
      return null;
    }
    return value.trim().toLowerCase();
  }

  private static String normalizeModel(String value) {
    if (StringUtils.isBlank(value)) {
      return null;
    }
    return value.trim();
  }

  private boolean isSupportedProvider(String provider) {
    return provider != null && supportedModels.containsKey(provider);
  }

  private boolean isSupportedModel(String model) {
    if (model == null) {
      return false;
    }
    for (List<String> models : supportedModels.values()) {
      if (models.contains(model)) {
        return true;
      }
    }
    return false;
  }

  private String findProviderForModel(String model) {
    if (model == null) {
      return null;
    }
    for (Map.Entry<String, List<String>> entry : supportedModels.entrySet()) {
      if (entry.getValue().contains(model)) {
        return entry.getKey();
      }
    }
    return null;
  }

  static final class Resolved {
    private final String provider;
    private final String model;

    Resolved(String provider, String model) {
      this.provider = provider;
      this.model = model;
    }

    String getProvider() {
      return provider;
    }

    String getModel() {
      return model;
    }
  }
}
