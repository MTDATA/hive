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
import org.apache.hadoop.io.Text;

import java.security.*;
import java.io.UnsupportedEncodingException;

/**
 * Calculate md5 of the string
 */
@Description(name = "md5",
    value = "_FUNC_(str) - Calculates an MD5 128-bit checksum for the string.",
    extended = "The value is returned as a string of 32 hex digits, or NULL if the argument was NULL. \n"
    + "The return value can, for example, be used as a hash key. \n"
    + "Example:\n"
    + "> SELECT _FUNC_('testing') from src limit 1;\n"
    + "'ae2b1fca515949e5d54fb22b8ed95575'\n"
    + "")
public final class UDFMd5 extends UDF {

	public Text evaluate(final Text s) {
	    if (s == null) {
                return null;
	    }
	    try {
	    	MessageDigest md = MessageDigest.getInstance("MD5");
	    	md.update(s.toString().getBytes("UTF8"));
	    	byte[] md5hash = md.digest();
	    	StringBuilder builder = new StringBuilder();
	    	for (byte b : md5hash) {
	    	    builder.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
	    	}
        return new Text(builder.toString());
	    } catch (NoSuchAlgorithmException nsae) {
	    	System.out.println("Cannot find digest algorithm");
                System.exit(1);
	    } catch (UnsupportedEncodingException usee) {
            System.out.println("Unsupport encoding exception");
            System.exit(1);
        }
	    return null;
	}
}
