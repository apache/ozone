/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.ha;

import org.apache.hadoop.hdds.scm.exceptions.SCMException;

import java.util.concurrent.atomic.AtomicLong;

/**
 * SequenceIDGenerator uses higher 30 bits to save the term, and lower 34 bits
 * to save a count (increase from 0). Each call of nextID() will increase the
 * count on the lower 34 bits by 1, thus SequenceIDGenerator always generates
 * unique number within a specific term.
 */
public class SequenceIDGenerator {
  private final long curTermOnHigher30Bits;
  private final AtomicLong counter = new AtomicLong(0L);
  private final long lower34BitsMask = 0x3FFFFFFFFL;

  public SequenceIDGenerator(long term) {
    // move term to higher 30 bits and save it into curTermOnHigher30Bits.
    curTermOnHigher30Bits = term << 34L;
  }

  public long nextID() throws SCMException {
    long l = counter.getAndIncrement();
    long countOnLower34Bits = l & lower34BitsMask;
    if (countOnLower34Bits != l) {
      throw new SCMException(
          String.format("ID generator generates a non unique id, " +
              "term:%s, count:%s ", curTermOnHigher30Bits >> 34L, l),
          SCMException.ResultCodes.INTERNAL_ERROR);
    }
    return countOnLower34Bits | curTermOnHigher30Bits;
  }
}
