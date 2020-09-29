package org.apache.hadoop.ozone.upgrade;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface UpgradeFinalizer<T> {

  Logger LOG = LoggerFactory.getLogger(UpgradeFinalizer.class);

  enum Status {
    ALREADY_FINALIZED,
    STARTING_FINALIZATION,
    FINALIZATION_IN_PROGRESS,
    FINALIZATION_DONE,
    FINALIZATION_REQUIRED
  }

  class StatusAndMessages {
    private Status status;
    private Collection<String> msgs;

    public StatusAndMessages(Status status, Collection<String> msgs) {
      this.status = status;
      this.msgs = msgs;
    }

    public Status status() { return status; }

    public Collection<String> msgs() { return msgs; }
  }

  StatusAndMessages STARTING_MSG = new StatusAndMessages(
      Status.STARTING_FINALIZATION,
      Arrays.asList("Starting Finalization")
  );

  StatusAndMessages FINALIZED_MSG = new StatusAndMessages(
      Status.ALREADY_FINALIZED, Collections.emptyList()
  );

  StatusAndMessages FINALIZATION_REQUIRED_MSG = new StatusAndMessages(
      Status.FINALIZATION_REQUIRED,
      Collections.emptyList()
  );

  StatusAndMessages finalize(String clientID, T service) throws IOException;

  StatusAndMessages reportStatus() throws IOException;

}
