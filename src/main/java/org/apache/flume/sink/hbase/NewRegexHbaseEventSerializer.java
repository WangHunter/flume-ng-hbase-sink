package org.apache.flume.sink.hbase;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

public class NewRegexHbaseEventSerializer
  implements HbaseEventSerializer
{
  public static final String REGEX_CONFIG = "regex";
  public static final String REGEX_DEFAULT = "(.*)";
  public static final String IGNORE_CASE_CONFIG = "regexIgnoreCase";
  public static final boolean INGORE_CASE_DEFAULT = false;
  public static final String COL_NAME_CONFIG = "colNames";
  public static final String COLUMN_NAME_DEFAULT = "payload";
  public static final String ROW_KEY_INDEX_CONFIG = "rowKeyIndex";
  public static final String ROW_KEY_NAME = "ROW_KEY";
  public static final String DEPOSIT_HEADERS_CONFIG = "depositHeaders";
  public static final boolean DEPOSIT_HEADERS_DEFAULT = false;
  public static final String CHARSET_CONFIG = "charset";
  public static final String CHARSET_DEFAULT = "UTF-8";
  protected static final AtomicInteger nonce = new AtomicInteger(0);
  protected static String randomKey = RandomStringUtils.randomAlphanumeric(10);
  protected byte[] cf;
  private byte[] payload;
  private List<byte[]> colNames = Lists.newArrayList();
  private Map<String, String> headers;
  private boolean regexIgnoreCase;
  private boolean depositHeaders;
  private Pattern inputPattern;
  private Charset charset;
  private int rowKeyIndex;

  public void configure(Context context)
  {
    String regex = context.getString("regex", "(.*)");
    System.out.println("##############################");
    System.out.println(regex);
    System.out.println("##############################");
    this.regexIgnoreCase = context.getBoolean("regexIgnoreCase", Boolean.valueOf(false)).booleanValue();
    this.depositHeaders = context.getBoolean("depositHeaders", Boolean.valueOf(false)).booleanValue();

    this.inputPattern = Pattern.compile(regex);

    this.charset = Charset.forName(context.getString("charset", 
      "UTF-8"));

    String colNameStr = context.getString("colNames", "payload");
    String[] columnNames = colNameStr.split(",");
    for (String s : columnNames) {
      this.colNames.add(s.getBytes(this.charset));
    }

    this.rowKeyIndex = context.getInteger("rowKeyIndex", Integer.valueOf(-1)).intValue();

    if (this.rowKeyIndex >= 0) {
      if (this.rowKeyIndex >= columnNames.length) {
        throw new IllegalArgumentException("rowKeyIndex must be less than num columns " + 
          columnNames.length);
      }
      if (!"ROW_KEY".equalsIgnoreCase(columnNames[this.rowKeyIndex]))
        throw new IllegalArgumentException("Column at " + this.rowKeyIndex + " must be " + 
          "ROW_KEY" + " and is " + columnNames[this.rowKeyIndex]);
    }
  }

  public void configure(ComponentConfiguration conf)
  {
  }

  public void initialize(Event event, byte[] columnFamily)
  {
    this.headers = event.getHeaders();
    this.payload = event.getBody();
    this.cf = columnFamily;
  }

  protected byte[] getRowKey(Calendar cal)
  {
    String rowKey = String.format("%s-%s-%s", new Object[] { Long.valueOf(cal.getTimeInMillis()), 
      randomKey, Integer.valueOf(nonce.getAndIncrement()) });
    return rowKey.getBytes(this.charset);
  }

  protected byte[] getRowKey() {
    return getRowKey(Calendar.getInstance());
  }

  public List<Row> getActions() throws FlumeException
  {
    System.out.println("进入方法");
    System.out.println(this.colNames.size());

    List actions = Lists.newArrayList();

    Matcher m = this.inputPattern.matcher(new String(this.payload, this.charset));

    m.find();
    System.out.println("m.groupCount()为："+m.groupCount());
    if (m.groupCount() != this.colNames.size()){
      return Lists.newArrayList();
    }
    
    try
    {
      byte[] rowKey;
//      byte[] rowKey;
      if (this.rowKeyIndex < 0)
        rowKey = getRowKey();
      else {
        rowKey = m.group(this.rowKeyIndex + 1).getBytes(Charsets.UTF_8);
      }
      Put put = new Put(rowKey);

      for (int i = 0; i < this.colNames.size(); i++) {
        System.out.println(m.group(i + 1));
        if (i != this.rowKeyIndex) {
          put.add(this.cf, (byte[])this.colNames.get(i), m.group(i + 1).getBytes(Charsets.UTF_8));
        }
      }
      
      
      if (this.depositHeaders) {
        for (Map.Entry entry : this.headers.entrySet()) {
          put.add(this.cf, ((String)entry.getKey()).getBytes(this.charset), ((String)entry.getValue()).getBytes(this.charset));
        }
      }
      actions.add(put);
    } catch (Exception e) {
      throw new FlumeException("Could not get row key!", e);
    }
    System.out.println("******************************");
    byte[] rowKey;
    return actions;
  }

  public List<Increment> getIncrements()
  {
    return Lists.newArrayList();
  }

  public void close()
  {
	  
  }
}