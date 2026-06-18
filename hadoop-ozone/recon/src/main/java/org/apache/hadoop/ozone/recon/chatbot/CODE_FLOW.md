# Recon Chatbot — Internal Code Flow

This document explains how the Recon chatbot processes a request, end to end. It is written
for reviewers who want to understand the moving parts without reading every file. It tracks the
current design: **direct in-JVM Recon calls + native LLM tool calls** (no HTTP loopback, no
prose-wrapped JSON parsing).

> Scope note: this describes production code under
> `hadoop-ozone/recon/src/main/java/org/apache/hadoop/ozone/recon/chatbot/`.

---

## 1. What the chatbot does

A user asks a natural-language question about an Ozone cluster ("how many datanodes are
healthy?", "list large keys in /vol1/bucket1", "what is Ozone?"). The chatbot:

1. Asks an LLM **which Recon API tool(s)** answer the question (or whether it can be answered
   directly as documentation).
2. **Executes** those Recon endpoint calls *in-process* (same JVM, no network loopback), with
   hard safety limits.
3. Feeds the raw JSON results back to the LLM to write a **human-readable summary**.

The whole thing is provider-agnostic: OpenAI, Gemini, and Anthropic are all reachable behind one
interface.

---

## 2. Component map

| Layer | Class | Responsibility |
|---|---|---|
| **API** | `api/ChatbotEndpoint` | JAX-RS REST endpoint (`/chatbot/*`); thread pool, timeouts, DTOs |
| **Orchestration** | `agent/ChatbotAgent` | The 3-step flow: tool selection → execution → summarization |
| **Tool catalog** | `agent/LlmToolSpecFactory` | Builds the native LLM tool specs (names, descriptions, param schemas) |
| **Helpers** | `agent/ChatbotUtils` | Pure helpers: prefix validation, record counting, classpath loading |
| **LLM contract** | `llm/LLMClient` | Provider-agnostic interface + DTOs (`ChatMessage`, `LLMResponse`, `ToolSpec`, `ToolCallRequest`) |
| **LLM impl** | `llm/LangChain4jDispatcher` | Only class that knows LangChain4j; builds/caches models, fires calls |
| **LLM routing** | `llm/LlmRouting` | Resolves requested (provider, model) → effective (provider, model) |
| **LLM params** | `llm/GenParams` | Immutable temperature + maxTokens for each LLM call |
| **Recon exec** | `recon/ReconQueryExecutor` | Single chokepoint: strips `prevKey`, clamps `limit` ≤ 1000, unwraps result |
| **Recon routing** | `recon/ReconEndpointRouter` | Maps a tool name → a direct call on a Recon JAX-RS endpoint bean |
| **Allowlist** | `recon/ReconApiAllowlist` | Security allowlist of permitted tool names |
| **Recon DTOs** | `recon/ReconQueryResult` / `ReconResponseUnwrapper` | Query result and JAX-RS `Response`→`JsonNode` conversion |
| **Security** | `security/CredentialHelper` | Reads API keys from JCEKS credential store / `ozone-site.xml` |
| **Config** | `ChatbotConfigKeys` | All `ozone.recon.chatbot.*` config keys + defaults |
| **Wiring** | `ChatbotModule` | Guice bindings (installed only when the feature is enabled) |
| **Error** | `ChatbotException` | Single typed exception wrapping all internal failures |

Prompt text lives outside Java as editable resources under
`hadoop-ozone/recon/src/main/resources/chatbot/`:
`recon-tool-selection-prompt-preamble.txt`, `recon-tool-semantics.md`,
`recon-summarization-prompt.txt`, `recon-fallback-prompt-template.txt`.

---

## 3. High-level flow

```
HTTP POST /api/v1/chatbot/chat
        │
        ▼
ChatbotEndpoint.chat(ChatRequest)         ── enabled? non-empty query?
        │  submit to bounded thread pool, block on Future.get(timeout)
        ▼
ChatbotAgent.processQuery(query, model, provider)
        │
        │  STEP 1 — tool selection (LLM call #1, with tool specs attached)
        ▼
   chooseToolsForQuery(...) ──► LLMClient.chatCompletion(...) ──► LangChain4jDispatcher
        │                                                     │
        │   returns ToolSelection (SINGLE / MULTI /           ▼
        │   DIRECT_ANSWER) or null (NO_SUITABLE_ENDPOINT)  provider API
        │
        ├── null            ──► handleFallback() (LLM call) ──► return
        ├── DIRECT_ANSWER   ──► return selection.answer()
        │
        │  STEP 2 — execute Recon calls (validated first)
        ▼
   validateToolCall(s)  ──► ReconQueryExecutor.execute(toolName, params)
        │                          │ strip prevKey, clamp limit ≤ 1000
        │                          ▼
        │                   ReconEndpointRouter.route(toolName, params)
        │                          │ direct call on a Recon endpoint bean
        │                          ▼
        │                   ReconResponseUnwrapper.unwrap(Response) → JsonNode
        │
        │  collected as Map<String, EndpointResult> (body + metadata)
        │
        │  STEP 3 — summarization (LLM call #2)
        ▼
   summarizeResponse(...) ──► LLMClient.chatCompletion(...) ──► provider API
        │
        ▼
   String summary ──► ChatResponse{response, success=true} ──► HTTP 200
```

---

## 4. Entry point: `ChatbotEndpoint`

**File:** `api/ChatbotEndpoint.java`

The starting point is the `chat` handler:

```java
@POST @Path("/chat") @Consumes(MediaType.APPLICATION_JSON)
public Response chat(ChatRequest request)
```

Sequence (`ChatbotEndpoint.java:168`):

1. **Guard checks** — returns `503` if the feature is disabled
   (`ChatbotConfigKeys.isChatbotEnabled`), `400` if the query is blank.
2. **Offload to a bounded thread pool.** Work is submitted to a dedicated
   `ThreadPoolExecutor` (`chatbotExecutor`) sized by
   `ozone.recon.chatbot.thread.pool.size` (default 5) with an `ArrayBlockingQueue` of
   `ozone.recon.chatbot.max.queue.size` (default 10). The Jetty thread then blocks on
   `Future.get(requestTimeoutMs)`. This caps how many Jetty threads chatbot work can ever
   occupy. (JAX-RS `@Suspended AsyncResponse` is unavailable in this container, hence the
   synchronous Future approach — see the class Javadoc.)
3. **Outcome mapping:**
   - Success → `ChatResponse{response, success=true}` as `200`.
   - `RejectedExecutionException` (pool + queue full) → `503`.
   - `TimeoutException` → cancel the future, return `504`
     (`ozone.recon.chatbot.request.timeout.ms`, default 3 min).
   - `ExecutionException` → `500` (generic message; cause is logged, not leaked).
   - `InterruptedException` → `503`.

The endpoint also exposes `GET /chatbot/health` and `GET /chatbot/models`. `userId` is masked
before logging (`sanitizeUserId`) so identities are not leaked into logs. `@PreDestroy shutdown()`
drains the pool (30 s grace) on Recon stop.

DTOs `ChatRequest` (`query`, `model`, `provider`, `userId`) and `ChatResponse`
(`response`, `success`) are static inner classes with `@JsonIgnoreProperties(ignoreUnknown=true)`.

---

## 5. Orchestration: `ChatbotAgent.processQuery`

**File:** `agent/ChatbotAgent.java:155`

This is the brain. It runs three steps and is the only place that wires LLM calls to Recon calls.

### Step 1 — Tool selection (`chooseToolsForQuery`, `:254`)

- Builds a **system prompt** = `recon-tool-selection-prompt-preamble.txt` + the semantic API
  guide `recon-tool-semantics.md` (`buildToolSelectionPrompt`, `:456`). The user prompt is just
  `"User Query: " + userQuery`.
- Generation params: `GenParams(0.1, 8192)` (deterministic tool selection).
- Calls `llmClient.chatCompletion(messages, model, provider, params, specs)` where `specs` come
  from `LlmToolSpecFactory.getToolSpecs()` — the full catalog of allowed Recon tools.
- **Interprets the response into a `ToolSelection`** (see §6):
  - **Native tool calls present** (modern models): one call → `ToolSelection.single(...)`;
    several → `ToolSelection.multi(...)` capped at `maxToolCalls`
    (`ozone.recon.chatbot.max.tool.calls`, default 5). Arguments JSON is parsed in
    `parseNativeToolCall` (`:328`).
  - **No tool calls, text contains `NO_SUITABLE_ENDPOINT`** → returns `null` (fallback signal).
  - **No tool calls, non-empty text** → `ToolSelection.directAnswer(content)` (documentation
    answer).
  - **Empty text** → `null`.

`processQuery` then branches on the result:
- `null` → `handleFallback(...)` (`:431`): a single LLM call using
  `recon-fallback-prompt-template.txt` (the `%s` is replaced by literal string replacement, not
  `String.format`, so a `%` in the user query cannot crash it).
- `DIRECT_ANSWER` → return `selection.answer()` verbatim (no Recon call).
- `SINGLE` / `MULTI` → validate, then execute (Step 2).

### Step 2 — Validate, then execute

- **Validation** (`validateToolCall`, `:535`; `validateToolCalls`, `:512`) runs *before* any
  execution and returns either `null` (allowed) or a user-facing message (blocked). Two layers:
  1. **Allowlist** — `ReconApiAllowlist.isRegistered(toolName)` must pass.
  2. **Safe-scope** — when `requireSafeScope` is true (default), `api_v1_keys_listKeys` requires
     a bucket-scoped `startPrefix` (`ChatbotUtils.isBucketScopedListKeysPrefix`).
- **Execution:**
  - **SINGLE** path (`:213`): call
    `reconQueryExecutor.execute(toolName, parameters)`, store the outcome in a one-entry
    `Map<String, EndpointResult>`.
  - **MULTI** path (`executeMultipleToolCalls`, `:350`): loop the calls; each success stores an
    `EndpointResult(body, metadata)`; each failure stores an `EndpointResult` whose body is
    `{"error": msg}` and whose metadata is `{"error": msg, "truncated": false}`. One failing
    call does not abort the others. Map keys are de-duplicated via `buildResponseKey` (`:571`),
    which appends `" [call N]"` when more than one call is present.

### Step 3 — Summarization (`summarizeResponse`, `:382`)

- System prompt = `recon-summarization-prompt.txt` (rules about truncation, sampling, Markdown
  formatting). User prompt is built by `buildSummarizationUserPrompt` (`:478`), which emits, per
  endpoint:
  ```
  Endpoint: <key>
  Response: <responseBody as JSON>
  ExecutionMetadata: <metadata as JSON>
  ```
- Generation params: `GenParams(0.3, 8192)`.
- Calls `llmClient.chatCompletion(...)`; if the model returns blank content, a fixed fallback
  sentence is returned. The returned string is the chatbot's final answer.

**Error handling:** every internal failure is wrapped in `ChatbotException` so the endpoint has a
single typed exception to map to `500`.

---

## 6. The `ToolSelection` and `EndpointResult` value types

Both are immutable private inner classes of `ChatbotAgent` (Java 8 — no records).

- **`ToolSelection`** (`:597`) is a tagged union with `enum Kind { SINGLE, MULTI, DIRECT_ANSWER }`
  and static factories `single(toolName, params)`, `multi(calls)`, `directAnswer(answer)`. A
  `null` `ToolSelection` is the "no suitable endpoint" sentinel that routes to the fallback.
  `processQuery` dispatches on `selection.kind()`.
- **`EndpointResult`** (`:653`) bundles one endpoint's `responseBody` (raw JSON, or an error map)
  with its `metadata` map. It exists because the multi-call **error branch** has no
  `ReconQueryResult` to carry — it needs to represent `{"error": ...}` for both body and
  metadata. `createExecutionMetadataMap` (`:579`) builds the success metadata
  `{recordsProcessed, truncated, maxRecords}` that feeds the holder.

These replaced an older flag-based `ToolCall` union and two parallel maps; the change is
internal-only and behavior-identical.

---

## 7. LLM dispatch: `LangChain4jDispatcher`

**File:** `llm/LangChain4jDispatcher.java` — the **only** class aware of LangChain4j.

A single `chatCompletion(messages, model, provider, params, tools)` handles both paths: text-only
when `tools` is `null` (summarization, fallback), tool-enabled when tool specs are supplied
(selection). It is a short pipeline that delegates the detail to focused private helpers:

1. **Resolve** provider/model via `LlmRouting.resolve(...)` (§8).
2. **Build (and cache) the model** (`buildModel`). The cache key is
   `provider:model:t=<temp>:m=<maxTokens>` (from the typed `GenParams`) so each distinct
   generation config builds a `ChatLanguageModel` (HTTP client + SSL context) once. On any call
   failure the entry is evicted so a bad config can't get stuck.
3. **Build the request** (`buildChatRequest`): translate internal `ChatMessage`s → LangChain4j
   messages (`translateMessages`: `system`→`SystemMessage`, `assistant`→`AiMessage`, everything
   else→`UserMessage`) and attach tools when provided — internal `ToolSpec` → LangChain4j
   `ToolSpecification` with a JSON-schema-like `ToolParameters` (`toLangChain4jToolSpecs`).
4. **Invoke the provider** (`invokeModel` → `chatModel.chat(...)`). A known LangChain4j 0.35.0
   quirk — reasoning models returning `content=null` throw
   `IllegalArgumentException("text cannot be null")` — is caught and surfaced as a `null`
   `ChatResponse`, which `chatCompletion` turns into an empty-text response (`emptyTextResponse`).
5. **Normalize** into `LLMResponse` (`toLLMResponse`): text content, model name, token counts
   (`TokenUsage`), and any native `ToolCallRequest`s.

**Provider construction** (`buildModelInternal`, `:356`): `openai`, `gemini`, `anthropic`.
Notably **Gemini is routed through `OpenAiChatModel`** against Google's OpenAI-compatible
`/v1beta/openai/` endpoint, because LangChain4j 0.35.0's native Gemini client ignores read
timeouts. API keys are always resolved via `CredentialHelper` (`resolveKey`, `:440`);
a missing key throws `LLMException` immediately.

At startup the dispatcher records which providers have a configured API key into
`supportedModels` (model lists are read from config so admins can update them without code
changes). `isAvailable()` is true iff at least one provider is configured; `getSupportedModels()`
flattens all configured model lists for the UI dropdown.

---

## 8. LLM routing: `LlmRouting`

**File:** `llm/LlmRouting.java` — pure resolution logic, no I/O.

`resolve(userProvider, userModel)` (`:48`):

1. Provider: use the requested provider if it's configured; else infer the provider from the
   requested model; else use the default provider.
2. Model: use the requested model if it appears in any configured list; else the default model.
3. **Consistency guard:** if the chosen model isn't valid for the chosen provider, reset *both*
   to the configured defaults (`ozone.recon.chatbot.provider` / `.default.model`, default
   `gemini` / `gemini-2.5-flash`).

Returns a `Resolved(provider, model)`.

---

## 9. Recon execution path

### `ReconQueryExecutor` (`recon/ReconQueryExecutor.java`)

The single chokepoint for every chatbot-initiated Recon call (`execute`, `:71`):

1. Strip `prevKey` — the chatbot never paginates.
2. Clamp `limit` to `MAX_RECORDS_PER_CALL = 1000` (`ChatbotUtils.parsePositiveInt` +
   `Math.min`). This bounds memory and keeps results within the LLM context window.
3. Delegate to `ReconEndpointRouter.route(toolName, params)`.
4. Unwrap via `ReconResponseUnwrapper.unwrap(response)` → `JsonNode`.
5. Estimate record count (`ChatbotUtils.estimateRecordCount`) and set `truncated = records >=
   effective` (a deliberately conservative heuristic — it may over-report, which only nudges the
   user to narrow scope).
6. Return `ReconQueryResult(json, records, truncated, effective)`.

### `ReconEndpointRouter` (`recon/ReconEndpointRouter.java`)

Maps each allowlisted tool name to a **direct in-JVM method call on an injected Recon JAX-RS
endpoint bean** (`ClusterStateEndpoint`, `NodeEndpoint`, `ContainerEndpoint`, `OMDBInsightEndpoint`,
`VolumeEndpoint`, `BucketEndpoint`, etc.). Dispatch is a `switch` on `toolName` in `route` (`:90`);
dense multi-arg calls delegate to small private helpers. An unknown tool name throws
`IllegalArgumentException`.

### `ReconResponseUnwrapper` (`recon/ReconResponseUnwrapper.java`)

Converts a JAX-RS `Response` to `JsonNode`: non-2xx status throws `IOException`; null
entity/response yields an empty object node; otherwise `ObjectMapper.valueToTree(entity)`.

### DTOs

- `ReconQueryResult` — `responseBody` (`JsonNode`) + `recordsProcessed` + `truncated` +
  `maxRecords`. The executor accepts `(toolName, parameters)` directly; there is no separate
  request object.

---

## 10. Security model

Defense is layered so that even a fully prompt-injected LLM cannot make the server do anything
unsafe:

1. **Prompt-level** — the tool-selection preamble explicitly tells the model the user message is
   untrusted and to ignore embedded instructions.
2. **Allowlist** (`ReconApiAllowlist`) — only the ~29 registered tool names can ever execute;
   checked in `validateToolCall` *before* any call. The router independently rejects unknown
   names.
3. **Safe-scope** — `api_v1_keys_listKeys` (potentially unbounded) requires a bucket-scoped
   `startPrefix` (`/<volume>/<bucket>` or deeper; rejects `/`, `..`, and shallow paths) when
   `requireSafeScope` is on (default).
4. **Record cap** — `ReconQueryExecutor` clamps every call to ≤ 1000 records and strips
   pagination cursors.
5. **Credential isolation** — `CredentialHelper` reads API keys from the JCEKS credential store
   first, falling back to `ozone-site.xml`. Keys are never accepted per-request.
6. **Resource bounds** — bounded thread pool + queue + per-request timeout (§4).

---

## 11. Wiring & enablement

`ChatbotModule` (Guice) binds `LLMClient → LangChain4jDispatcher` plus all singletons. Crucially
it is installed **only when the feature is enabled** — `ReconControllerModule:139` guards
`install(new ChatbotModule())` behind `ChatbotConfigKeys.isChatbotEnabled(...)`
(`ozone.recon.chatbot.enabled`, default `false`). So when the chatbot is off, none of its
bindings exist and it cannot affect Recon.

All `ozone.recon.chatbot.*` keys and defaults live in `ChatbotConfigKeys` — feature toggle,
provider/model defaults, per-provider API keys & base URLs & model lists, timeouts, thread
pool/queue sizes, max tool calls, and the safe-scope toggle.

---

## 12. Quick reference: which file to read for what

- "Where does a request enter?" → `api/ChatbotEndpoint.chat`
- "How is the answer assembled?" → `agent/ChatbotAgent.processQuery`
- "How does it pick a tool?" → `agent/ChatbotAgent.chooseToolsForQuery` + `agent/LlmToolSpecFactory`
- "How does it talk to the LLM?" → `llm/LangChain4jDispatcher`
- "How is a model/provider chosen?" → `llm/LlmRouting`
- "How is a Recon API actually called?" → `recon/ReconQueryExecutor` → `recon/ReconEndpointRouter`
- "What stops it from doing something unsafe?" → §10 (allowlist, safe-scope, caps, credentials)
- "What can it call?" → `recon/ReconApiAllowlist` + `agent/LlmToolSpecFactory`
