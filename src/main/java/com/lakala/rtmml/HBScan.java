package com.lakala.rtmml;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBScan {
  private static final byte[] F = Bytes.toBytes("F");
  private static final byte[] G = Bytes.toBytes("G");
  private static final byte[] x = Bytes.toBytes("x");
  private static final byte[] y = Bytes.toBytes("y");

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    HTable table = null;
    try {
      table = new HTable(conf, "rtmml");
    } catch (IOException e) {
      e.printStackTrace();
    }
    Scan scan = new Scan();

    scan.addColumn(F, Bytes.toBytes("99"));
    scan.addColumn(G, x);
    scan.addColumn(G, y);

    SingleColumnValueFilter filter =
        new SingleColumnValueFilter(F, Bytes.toBytes("99"), CompareOp.EQUAL, Bytes.toBytes("0"));
    scan.setFilter(filter);
    scan.setCaching(2000);
    scan.setBatch(100);
    ResultScanner scanner = table.getScanner(scan);
    for (Result result : scanner) {
      System.out.println(result);
    }

    scanner.close();
  }
}
