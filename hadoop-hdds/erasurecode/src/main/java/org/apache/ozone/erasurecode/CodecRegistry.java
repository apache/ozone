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

package org.apache.ozone.erasurecode;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.ozone.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.apache.ozone.erasurecode.rawcoder.NativeXORRawErasureCoderFactory;
import org.apache.ozone.erasurecode.rawcoder.RawErasureCoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class registers all coder implementations.
 *
 * {@link CodecRegistry} maps codec names to coder factories. All coder
 * factories are dynamically identified and loaded using ServiceLoader.
 */
@InterfaceAudience.Private
public final class CodecRegistry {

  private static final Logger LOG =
      LoggerFactory.getLogger(CodecRegistry.class);

  private static CodecRegistry instance = new CodecRegistry();
  private Map<String, List<RawErasureCoderFactory>> coderMap;
  private Map<String, String[]> coderNameMap;

  private CodecRegistry() {
    coderMap = new HashMap<>();
    coderNameMap = new HashMap<>();
    final ServiceLoader<RawErasureCoderFactory> coderFactories =
        ServiceLoader.load(RawErasureCoderFactory.class);
    updateCoders(coderFactories);
  }

  public static CodecRegistry getInstance() {
    return instance;
  }

  /**
   * Update coderMap and coderNameMap with iterable type of coder factories.
   * @param coderFactories
   */
  @VisibleForTesting
  void updateCoders(Iterable<RawErasureCoderFactory> coderFactories) {
    for (RawErasureCoderFactory coderFactory : coderFactories) {
      String codecName = coderFactory.getCodecName();
      List<RawErasureCoderFactory> coders = coderMap.get(codecName);
      if (coders == null) {
        coders = new ArrayList<>();
        coders.add(coderFactory);
        coderMap.put(codecName, coders);
        LOG.debug("Codec registered: codec = {}, coder = {}",
            coderFactory.getCodecName(), coderFactory.getCoderName());
      } else {
        Boolean hasConflit = false;
        for (RawErasureCoderFactory coder : coders) {
          if (coder.getCoderName().equals(coderFactory.getCoderName())) {
            hasConflit = true;
            LOG.error("Coder {} cannot be registered because its coder name " +
                    "{} has conflict with {}",
                coderFactory.getClass().getName(),
                coderFactory.getCoderName(), coder.getClass().getName());
            break;
          }
        }
        if (!hasConflit) {
          if (coderFactory instanceof NativeRSRawErasureCoderFactory
              || coderFactory instanceof NativeXORRawErasureCoderFactory) {
            coders.add(0, coderFactory);
          } else {
            coders.add(coderFactory);
          }
          LOG.debug("Codec registered: codec = {}, coder = {}",
              coderFactory.getCodecName(), coderFactory.getCoderName());
        }
      }
    }

    // update coderNameMap accordingly
    coderNameMap.clear();
    for (Map.Entry<String, List<RawErasureCoderFactory>> entry :
        coderMap.entrySet()) {
      String codecName = entry.getKey();
      List<RawErasureCoderFactory> coders = entry.getValue();
      coderNameMap.put(codecName, coders.stream().
          map(RawErasureCoderFactory::getCoderName)
          .toArray(String[]::new));
    }
  }

  /**
   * Get all coder names of the given codec.
   * @param codecName the name of codec
   * @return an array of all coder names, null if not exist
   */
  public String[] getCoderNames(String codecName) {
    String[] coderNames = coderNameMap.get(codecName);
    return coderNames;
  }

  /**
   * Get all coder factories of the given codec.
   * @param codecName the name of codec
   * @return a list of all coder factories, null if not exist
   */
  public List<RawErasureCoderFactory> getCoders(String codecName) {
    List<RawErasureCoderFactory> coders = coderMap.get(codecName);
    return coders;
  }

  /**
   * Get all codec names.
   * @return a set of all codec names
   */
  public Set<String> getCodecNames() {
    return coderMap.keySet();
  }

  /**
   * Get a specific coder factory defined by codec name and coder name.
   * @param codecName name of the codec
   * @param coderName name of the coder
   * @return the specific coder, null if not exist
   */
  public RawErasureCoderFactory getCoderByName(
      String codecName, String coderName) {
    List<RawErasureCoderFactory> coders = getCoders(codecName);

    // find the RawErasureCoderFactory with the name of coderName
    for (RawErasureCoderFactory coder : coders) {
      if (coder.getCoderName().equals(coderName)) {
        return coder;
      }
    }
    return null;
  }

  public RawErasureCoderFactory getCodecFactory(String codecName) {
    for (RawErasureCoderFactory factory : getCoders(codecName)) {
      return factory;
    }
    throw new IllegalArgumentException("There is no registered codec " +
        "factory for codec " + codecName);
  }
}
