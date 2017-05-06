package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;

/**
 * Find the bucket the first argument belongs to.  The buckets are
 * defined by the subsequent arguments.  Anything falling below the
 * first bucket is marked as 0, and anything above the last bucket is
 * marked as N, where N denotes the number of bucket parameters (i.e.,
 * total parameters - 1).  Ties are broken by choosing the lowest
 * bucket.
 */
@Description(name = "udfbucket",
             value = "_FUNC_(double, ...) - Find the bucket the first argument belongs to",
    extended = "Example:\n"
             + "  > SELECT BUCKET(foo, 0, 1, 2) FROM users;\n")
public class UDFLongest extends UDF {
    public String evaluate(String... strs) {
      String longest = null;
      for (int ii = 0; ii < strs.length; ++ii) {
        if (strs[ii] == null) {
          continue;
        }
        if (longest == null || strs[ii].length() > longest.length()) {
          longest = strs[ii];
        }
      }
      if (longest == null) {
        return null;
      } else {
        return new String(longest);
      }
    }
}
