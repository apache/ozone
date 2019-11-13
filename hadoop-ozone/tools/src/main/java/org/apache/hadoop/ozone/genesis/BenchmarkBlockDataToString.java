/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.genesis;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Benchmarks various implementations of {@link BlockData#toString}.
 */
@State(Scope.Benchmark)
public class BenchmarkBlockDataToString {

  @Param("1000")
  private int count;

  @Param({"112"})
  private int capacity;

  private List<BlockData> data;
  private List<String> values;

  @Setup
  public void createData() {
    ThreadLocalRandom rnd = ThreadLocalRandom.current();
    data = new ArrayList<>(count);
    values = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      BlockID blockID = new BlockID(rnd.nextLong(), rnd.nextLong());
      BlockData item = new BlockData(blockID);
      item.setBlockCommitSequenceId(rnd.nextLong());
      data.add(item);
      values.add(item.toString());
    }
  }

  @Benchmark
  public void usingToStringBuilderDefaultCapacity(
      BenchmarkBlockDataToString state, Blackhole sink) {
    for (int i = 0; i < state.count; i++) {
      BlockData item = state.data.get(i);
      String str = new ToStringBuilder(item, ToStringStyle.NO_CLASS_NAME_STYLE)
          .append("blockId", item.getBlockID().toString())
          .append("size", item.getSize())
          .toString();
      sink.consume(str);
      Preconditions.checkArgument(str.equals(state.values.get(i)));
    }
  }

  @Benchmark
  public void usingToStringBuilder(
      BenchmarkBlockDataToString state, Blackhole sink) {
    for (int i = 0; i < state.count; i++) {
      BlockData item = state.data.get(i);
      String str = new ToStringBuilder(item, ToStringStyle.NO_CLASS_NAME_STYLE,
          new StringBuffer(capacity))
          .append("blockId", item.getBlockID().toString())
          .append("size", item.getSize())
          .toString();
      sink.consume(str);
      Preconditions.checkArgument(str.equals(state.values.get(i)));
    }
  }

  @Benchmark
  public void usingSimpleStringBuilder(
      BenchmarkBlockDataToString state, Blackhole sink) {
    for (int i = 0; i < state.count; i++) {
      BlockData item = state.data.get(i);
      String str = new StringBuilder(capacity)
          .append("[")
          .append("blockId=")
          .append(item.getBlockID())
          .append(",size=")
          .append(item.getSize())
          .append("]")
          .toString();
      sink.consume(str);
      Preconditions.checkArgument(str.equals(state.values.get(i)));
    }
  }

  @Benchmark
  public void usingPushDownStringBuilder(
      BenchmarkBlockDataToString state, Blackhole sink) {
    for (int i = 0; i < state.count; i++) {
      BlockData item = state.data.get(i);
      StringBuilder sb = new StringBuilder(capacity);
      item.appendTo(sb);
      String str = sb.toString();
      sink.consume(str);
      Preconditions.checkArgument(str.equals(state.values.get(i)));
    }
  }

  @Benchmark
  public void usingConcatenation(
      BenchmarkBlockDataToString state, Blackhole sink) {
    for (int i = 0; i < state.count; i++) {
      BlockData item = state.data.get(i);
      String str = "[blockId=" +
          item.getBlockID() +
          ",size=" +
          item.getSize() +
          "]";
      sink.consume(str);
      Preconditions.checkArgument(str.equals(state.values.get(i)));
    }
  }

  @Benchmark
  public void usingInlineStringBuilder(
      BenchmarkBlockDataToString state, Blackhole sink) {
    for (int i = 0; i < state.count; i++) {
      BlockData item = state.data.get(i);
      BlockID blockID = item.getBlockID();
      ContainerBlockID containerBlockID = blockID.getContainerBlockID();
      String str = new StringBuilder(capacity)
          .append("[")
          .append("blockId=")
          .append("conID: ")
          .append(containerBlockID.getContainerID())
          .append(" locID: ")
          .append(containerBlockID.getLocalID())
          .append(" bcsId: ")
          .append(blockID.getBlockCommitSequenceId())
          .append(",size=")
          .append(item.getSize())
          .append("]")
          .toString();
      sink.consume(str);
      Preconditions.checkArgument(str.equals(state.values.get(i)));
    }
  }

}
