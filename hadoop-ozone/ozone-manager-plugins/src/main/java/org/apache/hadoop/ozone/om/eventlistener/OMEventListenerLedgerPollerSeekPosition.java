/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.eventlistener;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a helper class to get/set the seek position used by the
 * OMEventListenerLedgerPoller
 */
public class OMEventListenerLedgerPollerSeekPosition {
  public static final Logger LOG = LoggerFactory.getLogger(OMEventListenerLedgerPollerSeekPosition.class);

  private final AtomicReference<String> seekPosition;
  private PersistenceStrategy seekPositionSaver;

  public OMEventListenerLedgerPollerSeekPosition(PersistenceStrategy seekPositionSaver) {
    this.seekPosition = new AtomicReference(initSeekPosition(seekPositionSaver));
    this.seekPositionSaver = seekPositionSaver;
  }

  // TODO: other strategies will be availlable
  public static OMEventListenerLedgerPollerSeekPosition create(OzoneConfiguration conf) {

    // TODO: appropriate place to write this?
    Path filePath = Paths.get("/tmp/last-sent.key");
    return new OMEventListenerLedgerPollerSeekPosition(new LocalFilePersistenceStrategy(filePath));
  }

  private static String initSeekPosition(PersistenceStrategy seekPositionSaver) {
    try {
      String savedVal = seekPositionSaver.load();
      LOG.info("########## Loaded seek position {}", savedVal);
      return savedVal;
    } catch (IOException ex) {
      LOG.error("########## Error loading seek position", ex);
      return null;
    }
  }

  public String get() {
    return seekPosition.get();
  }

  public void set(String val) {
    LOG.info("############## SET SEEK POSIITION {}", val);
    // TODO: strictly we don't need to persist this for each event - we
    // could get away with doing so every X events and have a tolerance
    // for replaying a few events on a crash
    try {
      seekPositionSaver.save(val);
    } catch (IOException ex) {
      LOG.error("########## Error saving seek position", ex);
    }
    // NOTE: this in-memory view of the seek position needs to be kept
    // up to date because the OMEventListenerLedgerPoller has a
    // reference to it
    seekPosition.set(val);
  }

  interface PersistenceStrategy {
    String load() throws IOException;
    void save(String val) throws IOException;
  }

  static class LocalFilePersistenceStrategy implements PersistenceStrategy {
    private final Path path;

    public LocalFilePersistenceStrategy(Path path) {
      this.path = path;
    }

    public String load() throws IOException {
      try {
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        if (!lines.isEmpty()) {
          return lines.get(0);
        }
      } catch (NoSuchFileException ex) {
        // assume no existing file
      }
      return null;
    }

    public void save(String val) throws IOException {
      Path tmpFile = Paths.get(path.toString() + "-" + System.currentTimeMillis());
      // Write to a temp file and atomic rename to avoid corrupting the
      // file if we are interrupted by a restart while in the middle of
      // writing
      Files.write(tmpFile, val.getBytes());
      Files.move(tmpFile, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }
  }
}
