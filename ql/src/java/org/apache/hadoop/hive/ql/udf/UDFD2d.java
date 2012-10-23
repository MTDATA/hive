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
import java.util.Date;
import java.text.SimpleDateFormat;

/**
 * UDFDate.
 * Cast datekey to date, or otherwise
 * datekey to date, eg : select dt, d2d(dt, "", "-") from log.blog where dt=20120909 and hour='01' limit 3;
 * date to datekey, eg : select regdate, d2d(regdate, '-', '') from dim.user limit 3;
 *
 */
@Description(name = "d2d",
    value = "_FUNC_(expr) - Cast datekey to date, or otherwise",
    extended = "Example:\n "
    + "  > SELECT _FUNC_('20120909', '', '-') FROM src LIMIT 1;\n"
    + "  '2012-09-09'")
public class UDFD2d extends UDF {
    public String evaluate(
            final String s,
            final String sourceSep,
            final String targetSep) {
        if (s == null) {
            return null;
        }
        try {
            Date d = new SimpleDateFormat("yyyy" + sourceSep + "MM" + sourceSep + "dd").parse(s);
            return new SimpleDateFormat("yyyy" + targetSep + "MM" + targetSep + "dd").format(d);
        } catch(Exception ex) {
        }
        return null;
    }
}

