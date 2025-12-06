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

package org.apache.hadoop.hdds.server.events;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.scm.net.InnerNode;
import org.apache.hadoop.hdds.scm.net.NodeImpl;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple async event processing utility.
 * <p>
 * Event queue handles a collection of event handlers and routes the incoming
 * events to one (or more) event handler.
 */
public class EventQueue implements EventPublisher, AutoCloseable {

  private static final Logger LOG =
      LoggerFactory.getLogger(EventQueue.class);

  private static final String EXECUTOR_NAME_SEPARATOR = "For";

  private final Map<Event, Map<EventExecutor, List<EventHandler>>> executors =
      new HashMap<>();

  private final AtomicLong queuedCount = new AtomicLong(0);

  private final AtomicLong eventCount = new AtomicLong(0);

  private boolean isRunning = true;

  private static final ObjectWriter TRACING_SERIALIZER = buildSerializer();

  private boolean isSilent = false;
  private final String threadNamePrefix;

  public EventQueue() {
    threadNamePrefix = "";
  }

  public EventQueue(String threadNamePrefix) {
    this.threadNamePrefix = threadNamePrefix;
  }

  private static String serializeObject(Object payload) {
    try {
      return TRACING_SERIALIZER.writeValueAsString(payload);
    } catch (JsonProcessingException e) {
      return String.valueOf(payload);
    }
  }

  private static ObjectWriter buildSerializer() {
    ObjectMapper mapper = new ObjectMapper()
        .enable(SerializationFeature.INDENT_OUTPUT)
        .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
        .addMixIn(NodeImpl.class, DatanodeDetailsJacksonMixIn.class);

    SimpleModule module = new SimpleModule();

    module.addSerializer(Message.class, new JsonSerializer<Message>() {
      @Override
      public void serialize(Message msg, JsonGenerator gen, SerializerProvider sp) throws IOException {
        gen.writeObject(convertMessageToMap(msg));
      }

      private Object convertMessageToMap(Message msg) {
        Map<String, Object> fieldMap = new LinkedHashMap<>();
        for (Map.Entry<Descriptors.FieldDescriptor, Object> e : msg.getAllFields().entrySet()) {
          String fieldName = e.getKey().getName();
          Object value = convertField(e.getKey(), e.getValue());
          fieldMap.put(fieldName, value);
        }
        return fieldMap;
      }

      /**
       * Handles protobuf message fields.
       */
      private Object convertField(Descriptors.FieldDescriptor fd, Object value) {
        if (fd.isRepeated()) {
          List<?> fields = (List<?>) value;
          List<Object> result = new ArrayList<>();
          for (Object field : fields) {
            result.add(convertSingleValue(fd, field));
          }
          return result;
        }
        return convertSingleValue(fd, value);
      }

      /**
       * Converts a single field value to a JSON representation.
       */
      private Object convertSingleValue(Descriptors.FieldDescriptor fd, Object field) {
        switch (fd.getJavaType()) {
        case STRING:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
          return field;
        case ENUM:
          return ((Descriptors.EnumValueDescriptor) field).getName();
        case BYTE_STRING:
          ByteString bs = (ByteString) field;
          return Base64.getEncoder().encodeToString(bs.toByteArray());
        case MESSAGE:
          return convertMessageToMap((Message) field);
        default:
          return String.valueOf(field);
        }
      }
    });

    mapper.registerModule(module);
    return mapper.writerWithDefaultPrettyPrinter();
  }

  // The field parent in DatanodeDetails class has the circular reference
  // which will result in Gson infinite recursive parsing. We need to exclude
  // this field when generating json string for DatanodeDetails object
  abstract static class DatanodeDetailsJacksonMixIn {
    @JsonIgnore
    abstract InnerNode getParent();
  }

  /**
   * Add new handler to the event queue.
   * <p>
   * By default a separated single thread executor will be dedicated to
   * deliver the events to the registered event handler.
   *
   * @param event        Triggering event.
   * @param handler      Handler of event (will be called from a separated
   *                     thread)
   * @param <PAYLOAD>    The type of the event payload.
   * @param <EVENT_TYPE> The type of the event identifier.
   */
  public <PAYLOAD, EVENT_TYPE extends Event<PAYLOAD>> void addHandler(
      EVENT_TYPE event, EventHandler<PAYLOAD> handler) {
    Objects.requireNonNull(handler, "Handler should not be null.");
    validateEvent(event);
    String executorName = getExecutorName(event, handler);
    SingleThreadExecutor<PAYLOAD> executor =
        new SingleThreadExecutor<>(executorName, threadNamePrefix);
    this.addHandler(event, executor, handler);
  }

  /**
   * Return executor name for the given event and handler name.
   * @param event
   * @param eventHandler
   * @return executor name
   */
  public static <PAYLOAD> String getExecutorName(Event<PAYLOAD> event,
      EventHandler<PAYLOAD> eventHandler) {
    return StringUtils.camelize(event.getName()) + EXECUTOR_NAME_SEPARATOR
        + generateHandlerName(eventHandler);
  }

  private <EVENT_TYPE extends Event<?>> void validateEvent(EVENT_TYPE event) {
    Preconditions
        .checkArgument(!event.getName().contains(EXECUTOR_NAME_SEPARATOR),
            "Event name should not contain " + EXECUTOR_NAME_SEPARATOR
                + " string.");

  }

  private static <PAYLOAD> String generateHandlerName(
      EventHandler<PAYLOAD> handler) {
    if (!handler.getClass().isAnonymousClass()) {
      return handler.getClass().getSimpleName();
    } else {
      return handler.getClass().getName();
    }
  }

  /**
   * Add event handler with custom executor.
   *
   * @param event        Triggering event.
   * @param executor     The executor implementation to deliver events from a
   *                     separated threads. Please keep in your mind that
   *                     registering metrics is the responsibility of the
   *                     caller.
   * @param handler      Handler of event (will be called from a separated
   *                     thread)
   * @param <PAYLOAD>    The type of the event payload.
   * @param <EVENT_TYPE> The type of the event identifier.
   */
  public <PAYLOAD, EVENT_TYPE extends Event<PAYLOAD>> void addHandler(
      EVENT_TYPE event, EventExecutor<PAYLOAD> executor,
      EventHandler<PAYLOAD> handler) {
    if (!isRunning) {
      LOG.warn("Not adding handler for {}, EventQueue is not running", event);
      return;
    }
    validateEvent(event);
    String executorName = getExecutorName(event, handler);
    Preconditions.checkState(executorName.equals(executor.getName()),
        "Event Executor name is not matching the specified format. " +
            "It should be " + executorName + " but it is " +
            executor.getName());
    executors.putIfAbsent(event, new HashMap<>());
    executors.get(event).putIfAbsent(executor, new ArrayList<>());

    executors.get(event).get(executor).add(handler);
  }

  /**
   * Route an event with payload to the right listener(s).
   *
   * @param event   The event identifier
   * @param payload The payload of the event.
   * @throws IllegalArgumentException If there is no EventHandler for
   *                                  the specific event.
   */
  @Override
  public <PAYLOAD, EVENT_TYPE extends Event<PAYLOAD>> void fireEvent(
      EVENT_TYPE event, PAYLOAD payload) {

    if (!isRunning) {
      LOG.warn("Processing of {} is skipped, EventQueue is not running", event);
      return;
    }

    Map<EventExecutor, List<EventHandler>> eventExecutorListMap =
        this.executors.get(event);

    eventCount.incrementAndGet();
    if (eventExecutorListMap != null) {
      for (Map.Entry<EventExecutor, List<EventHandler>> executorAndHandlers :
          eventExecutorListMap.entrySet()) {
        for (EventHandler handler : executorAndHandlers.getValue()) {
          queuedCount.incrementAndGet();
          if (LOG.isTraceEnabled()) {
            String jsonPayload = serializeObject(payload);
            LOG.trace(
                "Delivering [event={}] to executor/handler {}: <json>{}</json>",
                event.getName(),
                executorAndHandlers.getKey().getName(),
                jsonPayload.replaceAll("\n", "\\\\n"));
          } else if (LOG.isDebugEnabled()) {
            LOG.debug("Delivering [event={}] to executor/handler {}: {}",
                event.getName(),
                executorAndHandlers.getKey().getName(),
                payload.getClass().getSimpleName());
          }
          executorAndHandlers.getKey()
              .onMessage(handler, payload, this);
        }
      }
    } else {
      if (!isSilent) {
        LOG.warn("No event handler registered for event {}", event);
      }
    }

  }

  /**
   * This is just for unit testing, don't use it for production code.
   * <p>
   * It waits for all messages to be processed. If one event handler invokes an
   * other one, the later one also should be finished.
   * <p>
   * Long counter overflow is not handled, therefore it's safe only for unit
   * testing.
   * <p>
   * This method is just eventually consistent. In some cases it could return
   * even if there are new messages in some of the handler. But in a simple
   * case (one message) it will return only if the message is processed and
   * all the dependent messages (messages which are sent by current handlers)
   * are processed.
   *
   * @param timeout Timeout in milliseconds to wait for the processing.
   */
  @VisibleForTesting
  public void processAll(long timeout) {
    long currentTime = Time.now();
    while (true) {

      if (!isRunning) {
        LOG.warn("Processing of event skipped. EventQueue is not running");
        return;
      }

      long processed = 0;

      Stream<EventExecutor> allExecutor = this.executors.values().stream()
          .flatMap(handlerMap -> handlerMap.keySet().stream());

      boolean allIdle =
          allExecutor.allMatch(executor -> executor.queuedEvents() == executor
              .successfulEvents() + executor.failedEvents());

      if (allIdle) {
        return;
      }

      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted exception while sleeping.", e);
        Thread.currentThread().interrupt();
      }

      if (Time.now() > currentTime + timeout) {
        throw new AssertionError(
            "Messages are not processed in the given timeframe. Queued: "
                + queuedCount.get() + " Processed: " + processed);
      }
    }
  }

  @Override
  public void close() {

    isRunning = false;

    Set<EventExecutor> allExecutors = this.executors.values().stream()
        .flatMap(handlerMap -> handlerMap.keySet().stream())
        .collect(Collectors.toSet());

    allExecutors.forEach(executor -> {
      try {
        executor.close();
      } catch (Exception ex) {
        LOG.error("Can't close the executor " + executor.getName(), ex);
      }
    });
  }

  /**
   * Dont log messages when there are no consumers of a message.
   * @param silent flag.
   */
  public void setSilent(boolean silent) {
    isSilent = silent;
  }

  @VisibleForTesting
  public Map<EventExecutor, List<EventHandler>> getExecutorAndHandler(
      Event event) {
    return executors.get(event);
  }
}
