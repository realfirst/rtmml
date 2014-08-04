package com.lakala.rtmml.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Describe class <code>HBasePutBolt</code> here.
 *
 * @author <a href="mailto:dingje.gm@gmail.com">Ding Jingen</a>
 * @version 1.0
 */
public class HBasePutBolt extends BaseBasicBolt {

  public static final byte[] CF = Bytes.toBytes("F");
  public static final byte[] LATEST_TIME = Bytes.toBytes("t");
  public static final byte[] ON_TIME = Bytes.toBytes("o");
  public static final byte[] ONE = Bytes.toBytes("1");

  HTableInterface table;

  @SuppressWarnings("rawtypes")
  @Override
  public final void prepare(final Map map,
                            final TopologyContext topologyContext) {
    Configuration conf = HBaseConfiguration.create();
    // conf.addResource("hbase-site.xml");
    try {
      HConnection conn = HConnectionManager.createConnection(conf);
      table = conn.getTable("rtmml");
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    try {
      String log = tuple.getString(0);
      Put put = null;
      String termId = null;
      String time = null;
      int start = 0;
      if (log.contains("iProcHearbeat结束")) {
        start = log.indexOf("TermId") + 7;
        if (log.charAt(start + 16) == ']') {
          termId = log.substring(start, start + 16);
          System.out.println(termId);
          put = new Put(Bytes.toBytes(termId));
        }
        time = log.substring(0, 8);
        put.add(CF, Bytes.toBytes(getColumnName(time)), ONE);
        put.add(CF, LATEST_TIME, Bytes.toBytes(time));
        table.put(put);
      }

      if (log.contains("iProcONOFFInfo开机时间")) {
        System.out.println("开机日志处理");
        start = log.indexOf("TermId") + 7;
        if (log.charAt(start + 16) == ']') {
          termId = log.substring(start, start + 16);
          put = new Put(Bytes.toBytes(termId));
        }
        time = log.substring(0, 8);
        put.add(CF, ON_TIME, Bytes.toBytes(time));
        table.put(put);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  /**
   * Describe <code>cleanup</code> method here.
   *
   */
  @Override
  public final void cleanup() {
    try {
      table.close();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
  }

  private static String getColumnName(String timeStamp) {
    String [] timeUnits = timeStamp.split(":");

    int hour = Integer.parseInt(timeUnits[0]);
    int minute = Integer.parseInt(timeUnits[1]);

    return ""  + (hour * 6  + minute / 10  + 1);
  }

}
