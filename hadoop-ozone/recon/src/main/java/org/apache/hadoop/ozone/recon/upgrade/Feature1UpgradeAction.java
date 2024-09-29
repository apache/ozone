package org.apache.hadoop.ozone.recon.upgrade;

public class Feature1UpgradeAction implements ReconUpgradeAction {
  @Override
  public void execute() throws Exception {
    // Logic for upgrading to version 1
    System.out.println("Executing Feature 1 upgrade: Adding new column to the table.");
    // Implement the database schema update or other upgrade logic here
  }
}
