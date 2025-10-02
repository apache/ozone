---
title: Distributed Tracing (OpenTelemetry)
summary: Use of OpenTelemetry for distributed tracing in Ozone.
date: 2025-09-19
jira: HDDS-13679
status: draft
---
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

## Distributed Tracing (OpenTelemetry)

Ozone is currently using OpenTracing with Jaeger as the destination. Now the OpenTracing project is deprecated and no longer supported. It is suggested to Migrate to OpenTelemetry which is a new standard and supports various tools, including Jaeger as a UI to show traces.

### Migration

Migration requires package and method changes with minimal impact. Jaeger will continue to work after migrating to OpenTelemetry.

-----

### OpenTelemetry Integration

#### Context

This keeps span and other information in the current context. This is thread-local. To retrieve the context, you can use `Context.current()`.

To transfer context across threads, it needs to be set via `context.makeCurrent()`. Refer to Trace propagation for details.

Contexts are created in the following ways:

* Creating a span with `noParent()`.
* Importing a trace from an external request.
* Manually creating a Context with parameters:
  ```
  Context rootContext = Context.root();
  Context newContextFromRoot = rootContext.with(myKey, "anotherValue");
  ```

#### Span

This holds the span of the flow to be monitored. It starts with `startSpan()` and ends with `end()`. It is stacked in the context for further inner/child span creation.

This needs to be set to the current context using `span.makeCurrent()`.

#### Scope

`context.makeCurrent()` / `span.makeCurrent()` returns a `Scope` object, which needs to be closed to release memory.

### Trace Propagation

* **Between threads:**
  * Manually: transfer `Context` to the thread and call `makeCurrent()`.
  * Context Wrapping for execution service:
    ```
    ExecutorService wrappedExecutor = Context.taskWrapping(Executors.newFixedThreadPool(1));
    ```
* **Across a network:**
  This uses `W3CTraceContextPropagator`, which is a standard way to encode trace information and transfer it over a network. It supports setting `HttpHeaders`, generating a string format, and other methods.
  For Ozone over gRPC, it can be encoded to a string and set to a Proto field (e.g., "traceId"). The same can be retrieved on the server and decoded back into a `Context`.

### Trace Failure

This is added for failures with the following sample steps:

```
span.addEvent("Failure has occurred" + ex.getMessage);
span.setStatus(StatusCode.ERROR);
```

### Tracing Hierarchy

Currently, traces are initiated as:

* Every remote call from Ozone client and shell.
* From Ozone Manager for `get blocks` to SCM.
* Remote call from Ozone client to DN for `put block`.

This results in every call being disjoint or having a small level of hierarchy, which does not present a complete flow.

As part of the call hierarchy, the goal is to:

* Combine all disjoint remote calls from the client into a single parent, for example, for `file create`, `write`, and `commit`.
* Include communication with SCM for `create file` or `allocate flow`.
* DN write should also be part of the same flow.

### Integration of Call Hierarchy from Users

Users supporting OpenTelemetry can set up their context for the flow. The Ozone Client will then continue from the user context as a parent, enabling end-to-end flow to be shown.

-----

### OpenTelemetry Span Kind

When a span is created, it is one of Client, Server, Internal, Producer, or Consumer. This span kind provides a hint to the tracing backend as to how the trace should be assembled.

* **Client:** Represents a synchronous outgoing remote call (e.g., an outgoing HTTP request or database call).
* **Server:** Represents a synchronous incoming remote call (e.g., an incoming HTTP request or remote procedure call).
* **Internal:** Represents operations which do not cross a process boundary (e.g., instrumenting a function call).
* **Producer:** Represents the creation of a job that may be asynchronously processed later (e.g., inserting into a job queue).
* **Consumer:** Represents the processing of a job created by a producer.

### Open Tracing Control Level

Tracing of a call flow can be categorized as:

* **External request tracing:** Initiated by a remote server, such as using the Ozone Client or the UI of Recon.
* **Internal requests:** Within Ozone components, like OM to SCM, and so on. These are initiated as part of a timer task.

A control flag is needed to identify which traces are required: external, internal, and other future categorizations.

### Integration of more flows
For performance analysis and debugging, trace can be added for various flows, such as:

- Datanode Heart Beat to SCM: need record trace only when Datanode initiate the trace context.
- Recon: trace for all requests from Recon UI to Ozone components such as Recon Server.
- Internal services like OM, when connecting to SCM, can initiate a call flow under a timer thread.

For ozone internal calls, trace should be initiated by caller as client span.
It should not be initiated as Server as it is not a remote call and it will be controlled within the ozone components.


### References
- [OpenTelemetry](https://opentelemetry.io/)

