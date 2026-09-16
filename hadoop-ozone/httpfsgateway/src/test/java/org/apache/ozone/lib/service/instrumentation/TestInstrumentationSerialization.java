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

package org.apache.ozone.lib.service.instrumentation;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ozone.lib.service.Instrumentation;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the {@code @JsonValue}-annotated snapshot types in
 * {@link InstrumentationService} serialize to the same JSON shape that the
 * former json-simple based serialization produced.
 */
public class TestInstrumentationSerialization {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testTimerSerialization() {
    InstrumentationService.Cron cron = new InstrumentationService.Cron();
    cron.start();
    cron.stop();
    InstrumentationService.Timer timer = new InstrumentationService.Timer(10);
    timer.addCron(cron);

    JsonNode node = MAPPER.valueToTree(timer);
    assertThat(node.isObject()).isTrue();
    assertThat(node.fieldNames()).toIterable()
        .containsExactly("lastTotal", "lastOwn", "avgTotal", "avgOwn");
    assertThat(node.get("lastTotal").isNumber()).isTrue();
    assertThat(node.get("avgOwn").isNumber()).isTrue();
  }

  @Test
  public void testVariableHolderSerialization() {
    InstrumentationService.VariableHolder<Long> holder =
        new InstrumentationService.VariableHolder<>(
            (Instrumentation.Variable<Long>) () -> 42L);

    JsonNode node = MAPPER.valueToTree(holder);
    assertThat(node.isObject()).isTrue();
    assertThat(node.fieldNames()).toIterable().containsExactly("value");
    assertThat(node.get("value").asLong()).isEqualTo(42L);
  }

  @Test
  public void testSamplerSerialization() {
    InstrumentationService.Sampler sampler =
        new InstrumentationService.Sampler();
    sampler.init(4, () -> 7L);
    sampler.sample();

    JsonNode node = MAPPER.valueToTree(sampler);
    assertThat(node.isObject()).isTrue();
    assertThat(node.fieldNames()).toIterable()
        .containsExactly("sampler", "size");
    assertThat(node.get("sampler").isNumber()).isTrue();
    assertThat(node.get("size").asInt()).isEqualTo(1);
  }
}
