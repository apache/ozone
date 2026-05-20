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

package org.apache.hadoop.ozone.om.eventlistener;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.eventlistener.s3.S3EventNotificationStrategy;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an implementation of OMEventListener which uses the
 * OMEventListenerLedgerPoller as a building block to periodically poll/consume
 * completed operations, serialize them to a S3 schema and produce them
 * to a kafka topic.
 */
public class OMEventListenerKafkaPublisher implements OMEventListener {
  public static final Logger LOG = LoggerFactory.getLogger(OMEventListenerKafkaPublisher.class);

  private static final String KAFKA_CONFIG_PREFIX = "ozone.om.plugin.kafka.";
  private static final String NOTIFICATION_STRATEGY_CONFIG = KAFKA_CONFIG_PREFIX + "notification.strategy";
  private static final Class<? extends OMEventListenerNotificationStrategy>
      DEFAULT_NOTIFICATION_STRATEGY = S3EventNotificationStrategy.class;
  private static final String KAFKA_SERVICE_INTERVAL_CONFIG = KAFKA_CONFIG_PREFIX + "service.interval";
  private static final String KAFKA_SERVICE_TIMEOUT_CONFIG = KAFKA_CONFIG_PREFIX + "service.timeout";
  private static final int COMPLETED_REQUEST_CONSUMER_CORE_POOL_SIZE = 1;

  private OMEventListenerLedgerPoller ledgerPoller;
  private KafkaClientWrapper kafkaClient;
  private OMEventListenerNotificationStrategy notificationStrategy;
  private OMEventListenerLedgerPollerSeekPosition seekPosition;

  @Override
  public void initialize(OzoneConfiguration conf, OMEventListenerPluginContext pluginContext) {
    Map<String, String> kafkaPropsMap = conf.getPropsMatchPrefixAndTrimPrefix(KAFKA_CONFIG_PREFIX);
    Properties kafkaProps = new Properties();
    kafkaProps.putAll(kafkaPropsMap);

    this.kafkaClient = new KafkaClientWrapper(kafkaProps);

    long kafkaServiceInterval = conf.getTimeDuration(
        KAFKA_SERVICE_INTERVAL_CONFIG, "2s", TimeUnit.MILLISECONDS);
    long kafkaServiceTimeout = conf.getTimeDuration(
        KAFKA_SERVICE_TIMEOUT_CONFIG, "5m", TimeUnit.MILLISECONDS);

    Class<? extends OMEventListenerNotificationStrategy> strategyClass = conf.getClass(
        NOTIFICATION_STRATEGY_CONFIG,
        DEFAULT_NOTIFICATION_STRATEGY,
        OMEventListenerNotificationStrategy.class);
    try {
      this.notificationStrategy = strategyClass.getDeclaredConstructor().newInstance();
    } catch (Exception ex) {
      LOG.error("Failed to instantiate notification strategy: {}. " +
          "Falling back to NoOp strategy.", strategyClass, ex);
      this.notificationStrategy = new NoOpOMEventListenerNotificationStrategy();
    }
    this.seekPosition = new OMEventListenerLedgerPollerSeekPosition();

    LOG.info("Creating OMEventListenerLedgerPoller with serviceInterval={}," +
        "serviceTimeout={}, seekPosition={}",
        kafkaServiceInterval, kafkaServiceTimeout,
        seekPosition);

    this.ledgerPoller = new OMEventListenerLedgerPoller(
        kafkaServiceInterval, TimeUnit.MILLISECONDS,
        COMPLETED_REQUEST_CONSUMER_CORE_POOL_SIZE,
        kafkaServiceTimeout, pluginContext, conf,
        seekPosition,
        this::handleCompletedRequest);
  }

  @Override
  public void start() {
    try {
      kafkaClient.initialize();
    } catch (IOException ex) {
      LOG.error("Failure initializing kafka client", ex);
      return;
    }

    ledgerPoller.start();
  }

  @Override
  public void stop() {
    try {
      kafkaClient.shutdown();
    } catch (IOException ex) {
      LOG.error("Failure shutting down kafka client", ex);
    }

    ledgerPoller.shutdown();
  }

  // callback called by OMEventListenerLedgerPoller
  public void handleCompletedRequest(OmCompletedRequestInfo completedRequestInfo) {
    LOG.debug("Processing {}", completedRequestInfo);

    List<String> eventsToSend = notificationStrategy.determineEventsForOperation(completedRequestInfo);

    // loop over events and send them to our kafka sink
    for (String event : eventsToSend) {
      if (event == null) {
        LOG.warn("Skipping null event for transaction {}", completedRequestInfo.getTrxLogIndex());
        continue;
      }
      try {
        kafkaClient.send(event);
      } catch (IOException ex) {
        LOG.error("Failure to send event {}", event, ex);
        return;
      }
    }

    // no errors so we can update the seek position
    seekPosition.set(String.valueOf(completedRequestInfo.getTrxLogIndex()));
  }

  static class KafkaClientWrapper {
    public static final Logger LOG = LoggerFactory.getLogger(KafkaClientWrapper.class);

    private final String topic;
    private final Properties kafkaProps;

    private KafkaProducer<String, String> producer;

    KafkaClientWrapper(Properties kafkaProps) {
      this.topic = (String) kafkaProps.get("topic");
      this.kafkaProps = kafkaProps;
    }

    public void initialize() throws IOException {
      LOG.info("Initializing kafka client for topic {}", topic);
      this.producer = new KafkaProducer<>(kafkaProps);

      ensureTopicExists();
    }

    public void shutdown() throws IOException {
      if (producer != null) {
        LOG.info("Closing kafka producer for topic {}", topic);
        producer.close(Duration.ofSeconds(10));
      }
    }

    public void send(String message) throws IOException {
      if (producer != null) {
        LOG.debug("Producing event {}", message);
        ProducerRecord<String, String> producerRecord =
            new ProducerRecord<>(topic, message);
        try {
          // TODO: Sequential blocking for every event ensures at-least-once delivery
          // but limits throughput. Consider batching async sends and calling
          // producer.flush() before updating the seek position for high-volume use cases.
          producer.send(producerRecord).get();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while sending message to Kafka", e);
        } catch (ExecutionException e) {
          throw new IOException("Failed to send message to Kafka", e);
        }
      } else {
        LOG.warn("Producing event {} [KAFKA DOWN]", message);
        throw new IOException("Kafka producer is not initialized");
      }
    }

    private void ensureTopicExists() {
      try (AdminClient adminClient = AdminClient.create(kafkaProps)) {
        LOG.info("Creating kafka topic: {}", this.topic);
        NewTopic newTopic = new NewTopic(this.topic, 1, (short) 1);
        // TODO: handle topic already exists failure
        adminClient.createTopics(Collections.singleton(newTopic)).all().get();
        adminClient.close();
      } catch (Exception ex) {
        LOG.error("Failed to create topic: {}", this.topic, ex);
      }
    }
  }
}
