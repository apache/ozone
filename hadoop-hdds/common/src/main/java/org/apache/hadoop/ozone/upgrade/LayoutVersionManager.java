package org.apache.hadoop.ozone.upgrade;

/**
 * Read Only interface to an Ozone component's Version Manager.
 */
public interface LayoutVersionManager {

  /**
   * Get the Current Metadata Layout Version.
   * @return MLV
   */
  int getMetadataLayoutVersion();

  /**
   * Get the Current Software Layout Version.
   * @return SLV
   */
  int getSoftwareLayoutVersion();

  /**
   * Does it need finalization?
   * @return true/false
   */
  boolean needsFinalization();

  /**
   * Is allowed feature?
   * @param layoutFeature feature object
   * @return true/false.
   */
  boolean isAllowed(LayoutFeature layoutFeature);

  /**
   * Is allowed feature?
   * @param featureName feature name
   * @return true/false.
   */
  boolean isAllowed(String featureName);

  /**
   * Get Feature given feature name.
   * @param name Feature name.
   * @return LayoutFeature instance.
   */
  LayoutFeature getFeature(String name);
}
