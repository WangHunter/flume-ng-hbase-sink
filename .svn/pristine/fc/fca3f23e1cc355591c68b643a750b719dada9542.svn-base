package org.apache.flume.sink.hbase;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;

public class NNZhRowKeyUtil {
	public static void main(String[] args) {
		System.out.println(getZXTime("2016/01/21 07:00:50"));
	}

	public static byte[] getAisinoJRLogsRowKey(String prefix,
			String payloadvalue) throws UnsupportedEncodingException {
		String[] strarr;
		byte[] result;
		strarr = payloadvalue.split("#");
		result = Bytes.add(Bytes.toBytes(prefix),
				Bytes.toBytes(Rowkeyip(strarr[5])),
				Bytes.toBytes(getTime(strarr[2])));
		return result;
	}

	public static byte[] getZHLogsRowKey(String prefix, String payloadvalue)
			throws UnsupportedEncodingException {
		String[] strarr;
		byte[] result = null;
		System.out.println("come into to get rowkey");

		if (payloadvalue != null) {
			System.out.println("come into if----");
			strarr = payloadvalue.split("#");
			// result = Bytes.add(Bytes.toBytes(prefix),
			// Bytes.toBytes(Rowkeyip(strarr[11])),
			// Bytes.toBytes(getTime(strarr[10])));
			result = Bytes.add(Bytes.toBytes(prefix),
					Bytes.toBytes(Rowkeyip(strarr[11])),
					Bytes.toBytes(getTime(strarr[2])));
		} else {
			System.out.println("come into eles---");
			System.out.println("prefix:" + prefix);
			result = Bytes.add(Bytes.toBytes(prefix),
					Bytes.toBytes(System.currentTimeMillis()));
			System.out.println("result:" + result);
		}
		System.out.println("result:----" + result);
		return result;
	}

	public static byte[] getZXRowKey(String prefix, String payloadvalue)
			throws UnsupportedEncodingException {
		String[] strarr;
		byte[] result = null;
		System.out.println("come into to get rowkey");
		try {

			if (payloadvalue != null) {
				strarr = payloadvalue.split("\\|");
				if (strarr.length < 12) {
					System.out.println("-------0-------");
					result = Bytes.add(Bytes.toBytes(strarr[0]),
							Bytes.toBytes(System.currentTimeMillis() / 1000),
							Bytes.toBytes(prefix));

				} else {
					if (strarr[13].replace(" ", "").trim().isEmpty()
							&& strarr[12].replace(" ", "").trim().isEmpty()) {
						System.out.println("-------1-------");
						result = Bytes.add(Bytes.toBytes(strarr[0]), Bytes
								.toBytes(System.currentTimeMillis() / 1000),
								Bytes.toBytes(prefix));
					}
					if (strarr[13].replace(" ", "").trim().isEmpty()
							&& !strarr[12].replace(" ", "").trim().isEmpty()) {
						System.out.println("-------2-------");
						result = Bytes.add(Bytes.toBytes(strarr[0]),
								Bytes.toBytes(getZXTime(strarr[12])),
								Bytes.toBytes(prefix));
					}
					if (strarr[12].replace(" ", "").trim().isEmpty()
							&& !strarr[13].replace(" ", "").trim().isEmpty()) {
						System.out.println("-------3-------");
						result = Bytes.add(Bytes.toBytes(strarr[0]),
								Bytes.toBytes(getZXTime(strarr[13])),
								Bytes.toBytes(prefix));
					}
					if (!strarr[12].replace(" ", "").trim().isEmpty()
							&& !strarr[13].replace(" ", "").trim().isEmpty()) {
						System.out.println("-------4-------");
						result = Bytes.add(Bytes.toBytes(strarr[0]),
								Bytes.toBytes(getZXTime(strarr[13])),
								Bytes.toBytes(prefix));
					}
				}

			} else {
				System.out.println("-------5-------");
				result = Bytes.add(Bytes.toBytes(prefix),
						Bytes.toBytes(System.currentTimeMillis()));
				System.out.println("result:" + result);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("result:----" + result);
		return result;
	}

	public static String getStrBody(String strbody) {
		int i = strbody.lastIndexOf("events  -");
		String strsub = strbody.substring(i + 10, strbody.length());
		return strsub;
	}

	public final static String getTime(String user_time) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date d;
		String str = null;
		try {
			d = sdf.parse(user_time);
			long l = d.getTime();
			str = String.valueOf(l);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return str;
	}

	public final static String getZHTime(String user_time) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date d;
		String str = null;
		try {
			d = sdf.parse(user_time);
			long l = d.getTime();
			str = String.valueOf(l);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return str;
	}

	public final static String getZXTime(String user_time) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date d;
		String str = null;
		try {
			d = sdf.parse(user_time);
			long l = d.getTime() / 1000;
			str = String.valueOf(l);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return str;
	}

	public static String Rowkeyip(String ip) {
		String[] strarr = ip.split("\\.");
		StringBuilder sb = new StringBuilder();
		sb.setLength(0);
		for (int i = 0; i < strarr.length; i++) {
			sb.append(strarr[i]);
		}
		return sb.toString();
	}

}
