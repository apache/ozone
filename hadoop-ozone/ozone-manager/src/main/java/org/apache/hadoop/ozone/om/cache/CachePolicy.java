package org.apache.hadoop.ozone.om.cache;


/**
 * Type like : LRU, LRU2(multi-level), MFU etc..
 */
public enum CachePolicy {

  DIR_LRU("DIR_LRU"),

  DIR_NOCACHE("DIR_NOCACHE"); // disable dir cache

  CachePolicy(String cachePolicy) {
    this.policy = cachePolicy;
  }

  private String policy;

  public String getPolicy() {
    return policy;
  }

  public static CachePolicy getPolicy(String policyStr) {
    for (CachePolicy policy : CachePolicy.values()) {
      if (policyStr.equalsIgnoreCase(policy.getPolicy())) {
        return policy;
      }
    }
    // defaulting to NO_CACHE
    return DIR_NOCACHE;
  }
}
