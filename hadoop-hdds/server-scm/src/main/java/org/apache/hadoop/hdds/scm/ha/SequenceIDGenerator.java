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
import org.apache.hadoop.util.ExitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * SequenceIDGenerator uses higher 30 bits to save the term, and lower 34 bits
 * to save a count (increase from 0). Each call of nextID() will increase the
 * count on the lower 34 bits by 1, thus SequenceIDGenerator always generates
 * unique number within a specific term.
 */
public class SequenceIDGenerator {
  private static final Logger LOG =
      LoggerFactory.getLogger(SequenceIDGenerator.class);

  // term on higher 30 bits
  private final long curTermOnHigherBits;
  private final AtomicLong counter = new AtomicLong(0L);
  // lower 34 bits mask
  private static final long LOWER_BITS_MASK = 0x3FFFFFFFFL;

  public SequenceIDGenerator(long term) {
    // move term to higher 30 bits and save it into curTermOnHigher30Bits.
    curTermOnHigherBits = term << 34L;
  }

  public long nextID() throws SCMException {
    long l = counter.getAndIncrement();
    long countOnLower34Bits = l & LOWER_BITS_MASK;
    if (countOnLower34Bits != l) {
      Throwable t = new SCMException(
          String.format("ID generator generates a non unique id, " +
              "term:%s, count:%s", curTermOnHigherBits >> 34L, l),
          SCMException.ResultCodes.INTERNAL_ERROR);
      // When IDs are exhausted for a term, we should terminate SCM to force
      // a leader re-election.
      LOG.error("Facing a fatal problem in SequenceIDGenerator", t);
      ExitUtil.terminate(1, t);
    }
    return countOnLower34Bits | curTermOnHigherBits;
  }
}
