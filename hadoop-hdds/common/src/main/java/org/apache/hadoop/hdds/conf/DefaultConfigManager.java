package org.apache.hadoop.hdds.conf;

import java.util.HashMap;
import java.util.Map;

public class DefaultConfigManager {
  private static final Map<String, Object> configDefaultMap = new HashMap<>();

  public static <T> void setConfigValue(String config, T value){
    T prevValue = getValue(config, value);
    if(!value.equals(prevValue)){
      throw new ConfigurationException(String.format("Setting conflicting " +
          "Default Configs old default Value: {} New Default Value:{}",
          prevValue, value));
    }
    configDefaultMap.putIfAbsent(config, value);
  }
  public static <T> T getValue(String config, T defaultValue){
    return (T) configDefaultMap.getOrDefault(config,defaultValue);
  }
}
