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

package org.apache.hadoop.ozone.recon.chatbot;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

/**
 * Configuration keys for Recon Chatbot service.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ChatbotConfigKeys {

  public static final String OZONE_RECON_CHATBOT_PREFIX = "ozone.recon.chatbot.";

  // ── Feature toggle ──────────────────────────────────────────
  public static final String OZONE_RECON_CHATBOT_ENABLED = OZONE_RECON_CHATBOT_PREFIX + "enabled";
  public static final boolean OZONE_RECON_CHATBOT_ENABLED_DEFAULT = false;

  // ── Provider selection ──────────────────────────────────────
  /**
   * Active default provider: openai, gemini, anthropic.
   */
  public static final String OZONE_RECON_CHATBOT_PROVIDER = OZONE_RECON_CHATBOT_PREFIX + "provider";
  public static final String OZONE_RECON_CHATBOT_PROVIDER_DEFAULT = "gemini";

  // ── Default model ───────────────────────────────────────────
  public static final String OZONE_RECON_CHATBOT_DEFAULT_MODEL = OZONE_RECON_CHATBOT_PREFIX + "default.model";
  public static final String OZONE_RECON_CHATBOT_DEFAULT_MODEL_DEFAULT = "gemini-2.5-flash";

  // ── HTTP timeout for provider calls ─────────────────────────
  public static final String OZONE_RECON_CHATBOT_TIMEOUT_MS = OZONE_RECON_CHATBOT_PREFIX + "timeout.ms";
  public static final int OZONE_RECON_CHATBOT_TIMEOUT_MS_DEFAULT = 120000;

  // ── Per-provider API keys (resolved via JCEKS / CredentialHelper) ──
  public static final String OZONE_RECON_CHATBOT_OPENAI_API_KEY = OZONE_RECON_CHATBOT_PREFIX + "openai.api.key";
  public static final String OZONE_RECON_CHATBOT_GEMINI_API_KEY = OZONE_RECON_CHATBOT_PREFIX + "gemini.api.key";
  public static final String OZONE_RECON_CHATBOT_ANTHROPIC_API_KEY = OZONE_RECON_CHATBOT_PREFIX
      + "anthropic.api.key";

  // ── Per-provider base URL overrides (optional) ──────────────
  public static final String OZONE_RECON_CHATBOT_OPENAI_BASE_URL = OZONE_RECON_CHATBOT_PREFIX + "openai.base.url";
  public static final String OZONE_RECON_CHATBOT_OPENAI_BASE_URL_DEFAULT = "https://api.openai.com";
  
  public static final String OZONE_RECON_CHATBOT_GEMINI_BASE_URL = OZONE_RECON_CHATBOT_PREFIX + "gemini.base.url";
  public static final String OZONE_RECON_CHATBOT_GEMINI_BASE_URL_DEFAULT = "https://generativelanguage.googleapis.com/v1beta/openai/";

  // ── Execution policy ────────────────────────────────────────
  // Total records aggregated for an answer are bounded by exec.max.pages * exec.page.size.
  public static final String OZONE_RECON_CHATBOT_EXEC_MAX_PAGES = OZONE_RECON_CHATBOT_PREFIX + "exec.max.pages";
  public static final int OZONE_RECON_CHATBOT_EXEC_MAX_PAGES_DEFAULT = 5;
  public static final String OZONE_RECON_CHATBOT_EXEC_PAGE_SIZE = OZONE_RECON_CHATBOT_PREFIX + "exec.page.size";
  public static final int OZONE_RECON_CHATBOT_EXEC_PAGE_SIZE_DEFAULT = 200;

  public static final String OZONE_RECON_CHATBOT_EXEC_REQUIRE_SAFE_SCOPE = OZONE_RECON_CHATBOT_PREFIX
      + "exec.require.safe.scope";
  public static final boolean OZONE_RECON_CHATBOT_EXEC_REQUIRE_SAFE_SCOPE_DEFAULT = true;

  // ── Agent configuration ─────────────────────────────────────
  public static final String OZONE_RECON_CHATBOT_MAX_TOOL_CALLS = OZONE_RECON_CHATBOT_PREFIX + "max.tool.calls";
  public static final int OZONE_RECON_CHATBOT_MAX_TOOL_CALLS_DEFAULT = 5;

  // ── ToolExecutor HTTP timeouts (loopback calls to Recon REST APIs) ───────
  /**
   * Connect timeout in milliseconds for loopback HTTP calls from ToolExecutor
   * to Recon's own REST APIs. Increase this on slow or heavily loaded clusters.
   */
  public static final String OZONE_RECON_CHATBOT_EXEC_CONNECT_TIMEOUT_MS =
      OZONE_RECON_CHATBOT_PREFIX + "exec.connect.timeout.ms";
  public static final int OZONE_RECON_CHATBOT_EXEC_CONNECT_TIMEOUT_MS_DEFAULT = 30_000;

  /**
   * Read timeout in milliseconds for loopback HTTP calls from ToolExecutor
   * to Recon's own REST APIs. Increase this when Recon APIs are slow due to
   * large dataset sizes (e.g. millions of unhealthy containers).
   */
  public static final String OZONE_RECON_CHATBOT_EXEC_READ_TIMEOUT_MS =
      OZONE_RECON_CHATBOT_PREFIX + "exec.read.timeout.ms";
  public static final int OZONE_RECON_CHATBOT_EXEC_READ_TIMEOUT_MS_DEFAULT = 30_000;

  // ── Async execution thread pool ──────────────────────────────
  /**
   * Number of threads in the dedicated thread pool used to execute chatbot
   * requests asynchronously, keeping Jetty's main thread pool free.
   * Each concurrent chatbot query occupies one thread for its full duration
   * (up to 2 LLM calls + up to 5 Recon API calls). Size this pool to the
   * maximum number of concurrent chatbot users you expect.
   */
  public static final String OZONE_RECON_CHATBOT_THREAD_POOL_SIZE =
      OZONE_RECON_CHATBOT_PREFIX + "thread.pool.size";
  public static final int OZONE_RECON_CHATBOT_THREAD_POOL_SIZE_DEFAULT = 5;

  /**
   * Maximum number of chatbot requests that can wait in the queue while all
   * threads are busy. Once this limit is reached, new requests are rejected
   * immediately with HTTP 503 (Service Unavailable) rather than queuing
   * indefinitely and consuming memory. Total in-flight chatbot load is bounded
   * by {@code thread.pool.size + max.queue.size}.
   */
  public static final String OZONE_RECON_CHATBOT_MAX_QUEUE_SIZE =
      OZONE_RECON_CHATBOT_PREFIX + "max.queue.size";
  public static final int OZONE_RECON_CHATBOT_MAX_QUEUE_SIZE_DEFAULT = 10;

  /**
   * Overall wall-clock timeout in milliseconds for a single chatbot request,
   * measured from the moment the HTTP request is received until a response must
   * be returned to the client. If the LLM or Recon API calls have not completed
   * within this window, the client receives an HTTP 504 Gateway Timeout response.
   *
   * <p>Default is 3 minutes — comfortably above the typical worst-case observed
   * latency (~90 s for slow preview models) while still protecting clients from
   * waiting indefinitely on a hung request.</p>
   */
  public static final String OZONE_RECON_CHATBOT_REQUEST_TIMEOUT_MS =
      OZONE_RECON_CHATBOT_PREFIX + "request.timeout.ms";
  public static final long OZONE_RECON_CHATBOT_REQUEST_TIMEOUT_MS_DEFAULT =
      3L * 60L * 1000L; // 3 minutes

  // ── Per-provider model lists (comma-separated, configurable) ──
  /**
   * Comma-separated list of OpenAI model names exposed via GET /chatbot/models.
   * Override this when OpenAI renames, adds, or retires models without requiring
   * a code change. Example: {@code gpt-4.1,gpt-4.1-mini,gpt-4.1-nano,o3}
   */
  public static final String OZONE_RECON_CHATBOT_OPENAI_MODELS =
      OZONE_RECON_CHATBOT_PREFIX + "openai.models";
  public static final String OZONE_RECON_CHATBOT_OPENAI_MODELS_DEFAULT =
      "gpt-4.1,gpt-4.1-mini,gpt-4.1-nano";

  /**
   * Comma-separated list of Google Gemini model names exposed via GET /chatbot/models.
   * Override this when Google renames, adds, or retires models without requiring
   * a code change. Example: {@code gemini-2.5-pro,gemini-2.5-flash}
   */
  public static final String OZONE_RECON_CHATBOT_GEMINI_MODELS =
      OZONE_RECON_CHATBOT_PREFIX + "gemini.models";
  public static final String OZONE_RECON_CHATBOT_GEMINI_MODELS_DEFAULT =
      "gemini-2.5-pro,gemini-2.5-flash,gemini-3-flash-preview,gemini-3.1-pro-preview";

  /**
   * Comma-separated list of Anthropic Claude model names exposed via GET /chatbot/models.
   * Override this when Anthropic renames, adds, or retires models without requiring
   * a code change. Example: {@code claude-opus-4-6,claude-sonnet-4-6,claude-haiku-4-6}
   */
  public static final String OZONE_RECON_CHATBOT_ANTHROPIC_MODELS =
      OZONE_RECON_CHATBOT_PREFIX + "anthropic.models";
  public static final String OZONE_RECON_CHATBOT_ANTHROPIC_MODELS_DEFAULT =
      "claude-opus-4-6,claude-sonnet-4-6";

  // ── Anthropic-specific headers ───────────────────────────────
  /**
   * Controls the Anthropic beta feature header sent with every request.
   * The default enables the extended 1M-token context window feature.
   * Set to empty string to disable sending the beta header entirely.
   */
  public static final String OZONE_RECON_CHATBOT_ANTHROPIC_BETA_HEADER =
      OZONE_RECON_CHATBOT_PREFIX + "anthropic.beta.header";
  public static final String OZONE_RECON_CHATBOT_ANTHROPIC_BETA_HEADER_DEFAULT =
      "context-1m-2025-08-07";

  /**
   * Returns whether the chatbot feature is enabled in the given configuration.
   * Centralised here so that both {@code ReconControllerModule} (Guice wiring)
   * and {@code ChatbotEndpoint} (request handling) use the same check without
   * duplicating the key name or default value.
   */
  public static boolean isChatbotEnabled(OzoneConfiguration configuration) {
    return configuration.getBoolean(
        OZONE_RECON_CHATBOT_ENABLED,
        OZONE_RECON_CHATBOT_ENABLED_DEFAULT);
  }

  /**
   * Never constructed.
   */
  private ChatbotConfigKeys() {

  }
}
