package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;

import java.util.Arrays;

/**
 * Number rows.
 */
@Description(name = "NUMBER_ROWS",
             value = "_FUNC_(key1, key2, ...) - Number rows starting at 1.  Whenever the value of any key changes the numbering is reset to 1.")

  public class UDFNumberRows extends UDF {
    Object[] previous_keys = null;
    int previous_index;

    public int evaluate(Object... keys) {
      if (previous_keys == null || !Arrays.equals(previous_keys, keys)) {
        previous_index = 0;
        previous_keys = keys.clone();
      }
      previous_index++;
      return previous_index;
    }
  }
