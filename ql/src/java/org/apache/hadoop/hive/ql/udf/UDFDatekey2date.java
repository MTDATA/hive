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

/**
 * Cast datekey to date, eg: 20120823 -> 2012-08-23
 */

@Description(name = "datekey2date",
    value = "_FUNC_(expr) - Cast datekey to date",
    extended = "Example:\n "
    + "  > SELECT _FUNC_('20120909') FROM src LIMIT 1;\n"
    + "  '2012-09-09'")
public class UDFDatekey2date extends UDF {
    public String evaluate(final String s) {
        return new UDFD2d().evaluate(s, "", "-");
    }
}