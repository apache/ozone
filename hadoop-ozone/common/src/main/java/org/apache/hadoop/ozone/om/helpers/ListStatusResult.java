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

package org.apache.hadoop.ozone.om.helpers;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * One page of a listStatus or listStatusLight result, with whether OM reported that it is the last page.
 *
 * @param <T> type of the listed entries
 */
public final class ListStatusResult<T> {
  private final List<T> statuses;
  private final boolean complete;

  private ListStatusResult(List<T> statuses, boolean complete) {
    this.statuses = statuses;
    this.complete = complete;
  }

  /** A page for which OM reported whether more entries follow. */
  public static <T> ListStatusResult<T> of(List<T> statuses, boolean truncated) {
    return new ListStatusResult<>(statuses, !truncated);
  }

  /** A page for which it is unknown whether more entries follow, for example from an OM that does not report it. */
  public static <T> ListStatusResult<T> unknown(List<T> statuses) {
    return new ListStatusResult<>(statuses, false);
  }

  public List<T> getStatuses() {
    return statuses;
  }

  /** True only if OM reported that no entries follow this page; false if more may follow or it is unknown. */
  public boolean isComplete() {
    return complete;
  }

  /** The same page with every entry converted by {@code converter}. */
  public <R> ListStatusResult<R> map(Function<? super T, ? extends R> converter) {
    List<R> converted = new ArrayList<>(statuses.size());
    for (T status : statuses) {
      converted.add(converter.apply(status));
    }
    return new ListStatusResult<>(converted, complete);
  }
}
