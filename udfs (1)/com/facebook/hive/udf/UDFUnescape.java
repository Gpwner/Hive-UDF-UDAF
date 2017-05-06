package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.commons.lang.StringEscapeUtils;

/**
 * UDF to extract a specific group identified by a java regex.
 * Note that if a regexp has a backslash ('\'), then need to specify '\\'
 * For example, regexp_extract('100-200', '(\\d+)-(\\d+)', 1) will return '100'
 */
@Description(name = "udfunescape",
             value = "_FUNC_(string) - Unescape the string.")
  public class UDFUnescape extends UDF {
    public String evaluate(String s) {
      if (s == null) {
        return null;
      }
      return StringEscapeUtils.unescapeJava(s);
   }
  }
