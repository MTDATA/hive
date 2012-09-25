/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.meituan.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFSubstring_index.
 * ref: http://dev.mysql.com/doc/refman/5.0/en/string-functions.html#function_substring-index
 *
 */
@Description(name = "substring_index",
    value = "_FUNC_(str, delim, count) - returns the substring of str before"
    + " count occurrences of the delimiter delim. ",
    extended = "If count is positive, everything to the left of the final delimiter (counting from the left) is returned."
    + " If count is negative, everything to the right of the final delimiter (counting from the right) is returned.\n"
    + "Example:\n "
    + "  > SELECT _FUNC_('www.mysql.com', '.' 2) FROM src LIMIT 1;\n"
    + "  'www.mysql'\n"
    + "  > SELECT _FUNC_('www.mysql.com', -2) FROM src LIMIT 1;\n"
    + "  'mysql.com'\n")
public class UDFSubstring_index extends UDF {
  final Text r = new Text();
  
  public Text evaluate(final Text _input, final Text _delim, final IntWritable _pos) {
    if ((_input == null) || (_delim == null) || (_pos == null)) {
      return null;
    }
    
    final int pos = _pos.get();
    final String input = _input.toString();
    final String delim = _delim.toString();
    
    r.clear();

    if (pos == 0 || delim.equals("")) {
      return r;
    }

    int count = 0;

    if (pos > 0) {
      int k = -1;
      
      while (count++ < pos) {
        k = input.indexOf(delim, k + 1);
        if (k < 0) {
          r.set(input);
          return r;
        }
     }
      
      r.set(input.substring(0, k));
      return r;
    }
    
    int k = input.length() + 1;

    while (count-- > pos) {
      k = input.lastIndexOf(delim, k - 1);
      if (k < 0) {
        r.set(input);
        return r;
      }
    }

    r.set(input.substring(k + delim.length()));
    return r;
  }
}
