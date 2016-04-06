/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.hbase;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Utility class for users to generate their own keys. Any key can be used, this
 * is just a utility that provides a set of simple keys.
 * 
 * 
 */
public class SimpleRowKeyGenerator {

	public static byte[] getUUIDKey(String prefix)
			throws UnsupportedEncodingException {
		return (prefix + UUID.randomUUID().toString()).getBytes("UTF8");
	}

	public static byte[] getRandomKey(String prefix)
			throws UnsupportedEncodingException {
		return (prefix + String.valueOf(new Random().nextLong()))
				.getBytes("UTF8");
	}

	public static byte[] getTimestampKey(String prefix)
			throws UnsupportedEncodingException {
		return (prefix + String.valueOf(System.currentTimeMillis()))
				.getBytes("UTF8");
	}

	public static byte[] getNanoTimestampKey(String prefix)
			throws UnsupportedEncodingException {
		return (prefix + String.valueOf(System.nanoTime())).getBytes("UTF8");
	}

	public static byte[] getCustomKey(String prefix)
			throws UnsupportedEncodingException {
		return (SimpleRowKeyGenerator.MD5(prefix)).getBytes("UTF8");
	}

	// TODO HBASE-ROWKEY
	public static byte[] getCustomHashKey(String prefix, String payloadvalue)
			throws UnsupportedEncodingException {
		String[] strarr;
		byte[] result;
		strarr = payloadvalue.split("#");
		// System.out.println("payloadvalue------------------------------------------------------------------------>"+payloadvalue);
		// System.out.println("strarr[4]==========================>"+strarr[4]);
		// System.out.println("getTime(strarr[4])------------------>"+getTime(strarr[4]));
		result = Bytes.add(Bytes.toBytes(prefix), Bytes.toBytes(strarr[0]),
				Bytes.toBytes(getTime(strarr[2])));
		return result;
	}

	public final static String MD5(String s) {
		char hexDigits[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
				'A', 'B', 'C', 'D', 'E', 'F' };
		try {
			byte[] btInput = s.getBytes();

			MessageDigest mdInst = MessageDigest.getInstance("MD5");

			mdInst.update(btInput);

			byte[] md = mdInst.digest();

			int j = md.length;
			char str[] = new char[j * 2];
			int k = 0;
			for (int i = 0; i < j; i++) {
				byte byte0 = md[i];
				str[k++] = hexDigits[byte0 >>> 4 & 0xf];
				str[k++] = hexDigits[byte0 & 0xf];
			}
			return new String(str);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public final static String getTime(String user_time) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date d;
		String str = null;
		try {
			d = sdf.parse(user_time);
			long l = d.getTime();
			;

			str = String.valueOf(l);

		} catch (ParseException e) {
			e.printStackTrace();
		}
		return str;
	}

	public static void main(String[] args) {
		// System.out.println(SimpleRowKeyGenerator.MD5("13575873109"));
		// System.out.println(getTime("2015/8/5 16:16:44"));
	}
}
