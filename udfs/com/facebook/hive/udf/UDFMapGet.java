package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;

import java.util.HashMap;

/**
 * Fix the fact that [] doesn't work dynamically.
 */
@Description(name = "MAP_GET",
             value = "_FUNC_(map, key) - Dynamic version of map[key].")

  public class UDFMapGet extends UDF {
    public Double evaluate(HashMap<String, Double> map, String key) {
      if (map == null || key == null) {
        return null;
      }
      return map.get(key);
    }
    public String evaluate(HashMap<String, String> map, String key) {
      if (map == null || key == null) {
        return null;
      }
      return map.get(key);
    }
    public Long evaluate(HashMap<String, Long> map, String key) {
      if (map == null || key == null) {
        return null;
      }
      return map.get(key);
    }
    public String evaluate(HashMap<Long, String> map, Long key) {
      if (map == null || key == null) {
        return null;
      }
      return map.get(key);
    }
    public Integer evaluate(HashMap<String, Integer> map, String key) {
      if (map == null || key == null) {
        return null;
      }
      return map.get(key);
    }
  }
