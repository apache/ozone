package org.apache.hadoop.ozone.admin.scm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.common.helpers.DeletedBlocksTransactionInfoWrapper;
import picocli.CommandLine;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Handler of resetting expired deleted blocks from SCM side.
 */
@CommandLine.Command(
    name = "reset",
    description = "Reset the retry count of failed DeletedBlocksTransaction",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ResetDeletedBlockRetryCountSubcommand extends ScmSubcommand {

  @CommandLine.ArgGroup(multiplicity = "1")
  private TransactionsOption group;

  static class TransactionsOption {
    @CommandLine.Option(names = {"-a", "--all"},
        description = "Reset all expired deleted block transaction retry" +
            " count from -1 to 0.")
    private boolean resetAll;

    @CommandLine.Option(names = {"-l", "--list"},
        split = ",",
        paramLabel = "txID",
        description = "Reset the only given deletedBlock transaction ID" +
            " list. Example: 100,101,102.(Separated by ',')")
    private List<Long> txList;

    @CommandLine.Option(names = {"-i", "--in"},
        description = "Use file as input, need to be JSON Array format and " +
            "contains multi \"txID\" key. Example: [{\"txID\":1},{\"txID\":2}]")
    private String fileName;
  }

  @Override
  public void execute(ScmClient client) throws IOException {
    int count;
    if (group.resetAll) {
      count = client.resetDeletedBlockRetryCount(new ArrayList<>());
    } else if (group.fileName != null) {
      ObjectMapper objectMapper = new ObjectMapper();
      List<Long> txIDs;
      try (FileInputStream in = new FileInputStream(group.fileName)) {
        List<DeletedBlocksTransactionInfoWrapper> txns = objectMapper.readValue(in, new TypeReference<List<DeletedBlocksTransactionInfoWrapper>>() {});
        txIDs = txns.stream()
            .map(DeletedBlocksTransactionInfoWrapper::getTxID)
            .sorted()
            .distinct()
            .collect(Collectors.toList());
        System.out.println("Num of loaded txIDs: " + txIDs.size());
        if (!txIDs.isEmpty()) {
          System.out.println("The first loaded txID: " + txIDs.get(0));
          System.out.println("The last loaded txID: " +
              txIDs.get(txIDs.size() - 1));
        }
      } // No need to catch JsonIOException or JsonSyntaxException as Jackson throws IOException for parsing errors
      count = client.resetDeletedBlockRetryCount(txIDs);
    } else {
      if (group.txList == null || group.txList.isEmpty()) {
        System.out.println("TransactionId list should not be empty");
        return;
      }
      count = client.resetDeletedBlockRetryCount(group.txList);
    }
    System.out.println("Reset " + count + " deleted block transactions in" +
        " SCM.");
  }
}
