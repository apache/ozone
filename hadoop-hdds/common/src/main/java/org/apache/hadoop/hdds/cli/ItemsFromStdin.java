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

package org.apache.hadoop.hdds.cli;

import static java.util.Collections.unmodifiableList;

import jakarta.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Parameter for specifying list of items, reading from stdin if "-" is given as first item. */
public abstract class ItemsFromStdin<T> implements Iterable<T> {

  protected static final String FORMAT_DESCRIPTION =
      ": one or more, separated by spaces. To read from stdin, specify '-' and supply one item per line.";

  private List<T> items;

  protected void setItems(List<String> arguments, Function<String, T> mapper) {
    items = readItemsFromStdinIfNeeded(arguments, mapper);
  }

  public List<T> getItems() {
    return unmodifiableList(items);
  }

  @Nonnull
  @Override
  public Iterator<T> iterator() {
    return items.iterator();
  }

  public int size() {
    return items.size();
  }

  private List<T> readItemsFromStdinIfNeeded(List<String> parameters, Function<String, T> mapper) {
    if (parameters.isEmpty() || !"-".equals(parameters.iterator().next())) {
      return parameters.stream().map(mapper).collect(Collectors.toList());
    }

    List<T> items = new ArrayList<>();
    Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.name());
    while (scanner.hasNextLine()) {
      items.add(mapper.apply(scanner.nextLine().trim()));
    }
    return items;
  }
}
