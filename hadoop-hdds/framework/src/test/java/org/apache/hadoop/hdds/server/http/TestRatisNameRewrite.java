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

package org.apache.hadoop.hdds.server.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test Ratis metrics renaming.
 */
public class TestRatisNameRewrite {

  private static Stream<Arguments> parameters() {
    return Stream.of(
        arguments(
            "ratis.log_appender"
                + ".851cb00a-af97-455a-b079-d94a77d2a936@group-C14654DE8C2C"
                + ".follower_65f881ea-8794-403d-be77-a030ed79c341_match_index",
            "ratis.log_appender.follower_match_index",
            new String[] {"instance", "group", "follower"},
            new String[] {"851cb00a-af97-455a-b079-d94a77d2a936",
                "group-C14654DE8C2C",
                "65f881ea-8794-403d-be77-a030ed79c341"}
        ),
        arguments(
            "ratis_grpc.log_appender.72caaf3a-fb1c-4da4-9cc0-a2ce21bb8e67@group"
                + "-72caaf3a-fb1c-4da4-9cc0-a2ce21bb8e67"
                + ".grpc_log_appender_follower_75fa730a-59f0-4547"
                + "-bd68-216162c263eb_latency",
            "ratis_grpc.log_appender.grpc_log_appender_follower_latency",
            new String[] {"instance", "group", "follower"},
            new String[] {"72caaf3a-fb1c-4da4-9cc0-a2ce21bb8e67",
                "group-72caaf3a-fb1c-4da4-9cc0-a2ce21bb8e67",
                "75fa730a-59f0-4547-bd68-216162c263eb"}
        ),
        arguments(
            "ratis_core.ratis_log_worker.72caaf3a-fb1c-4da4-9cc0-a2ce21bb8e67"
                + ".dataQueueSize",
            "ratis_core.ratis_log_worker.dataQueueSize",
            new String[] {"instance"},
            new String[] {"72caaf3a-fb1c-4da4-9cc0-a2ce21bb8e67"}
        ),
        arguments(
            "ratis_grpc.log_appender.8e505d6e-12a4-4660-80e3-eb735879db06"
                + "@group-49616B7F02CE.grpc_log_appender_follower_a4b099a7"
                + "-511f-4fef-85bf-b9eeddd7c270_latency",
            "ratis_grpc.log_appender.grpc_log_appender_follower_latency",
            new String[] {"instance", "group", "follower"},
            new String[] {"8e505d6e-12a4-4660-80e3-eb735879db06",
                "group-49616B7F02CE", "a4b099a7-511f-4fef-85bf-b9eeddd7c270"}
        ),
        arguments(
            "ratis_grpc.log_appender.8e505d6e-12a4-4660-80e3-eb735879db06"
                + "@group-49616B7F02CE.grpc_log_appender_follower_a4b099a7"
                + "-511f-4fef-85bf-b9eeddd7c270_success_reply_count",
            "ratis_grpc.log_appender"
                + ".grpc_log_appender_follower_success_reply_count",
            new String[] {"instance", "group", "follower"},
            new String[] {"8e505d6e-12a4-4660-80e3-eb735879db06",
                "group-49616B7F02CE", "a4b099a7-511f-4fef-85bf-b9eeddd7c270"}
        )
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void normalizeRatisMetricName(String originalName, String expectedName,
      String[] expectedTagNames, String[] expectedTagValues) {

    List<String> names = new ArrayList<>();
    List<String> values = new ArrayList<>();

    String cleanName = new RatisNameRewriteSampleBuilder()
        .normalizeRatisMetric(originalName, names, values);

    assertEquals(expectedName, cleanName);
    assertEquals(Arrays.asList(expectedTagNames), names);
    assertEquals(Arrays.asList(expectedTagValues), values);

  }
}
