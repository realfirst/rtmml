package com.lakala.rtmml.mr;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HBPsamXY {
  static public class HBPsamXYMapper
      extends Mapper<AvroKey<GenericRecord>, NullWritable,
      ImmutableBytesWritable, KeyValue> {
    private final byte[] G = Bytes.toBytes("G");
    private final byte[] x = Bytes.toBytes("x");
    private final byte[] y = Bytes.toBytes("y");

    private final byte[] F = Bytes.toBytes("F");
    private final byte[] ZERO = Bytes.toBytes("0");
    
    private static final String PSAM = "PSAM";
    private static final String LONGITUDE = "LONGITUDE";
    private static final String LATITUDE = "LATITUDE";


    @Override
    public final void map(AvroKey<GenericRecord> key,
                          NullWritable ignore, Context context)
        throws IOException, InterruptedException {
      GenericRecord record = key.datum();

      byte[] bRowkey = ((String)record.get(PSAM)).getBytes();
      ImmutableBytesWritable rowkey = new ImmutableBytesWritable(bRowkey);
      KeyValue kvX = new KeyValue(bRowkey, G, x, ((String)record.get(LONGITUDE)).getBytes());
      context.write(rowkey, kvX);

      KeyValue kvY = new KeyValue(bRowkey, G, y, ((String)record.get(LATITUDE)).getBytes());
      context.write(rowkey, kvY);

      for (int i = 0; i < 144; i++) {
        context.write(rowkey, new KeyValue(bRowkey, F, Bytes.toBytes("" + (i + 1)), ZERO));
      }
    }
  }

  static public class HBPsamXYReducer
      extends Reducer<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, KeyValue> {

    @Override
    protected void reduce(ImmutableBytesWritable key,
                          Iterable<KeyValue> values,
                          Context context)
        throws IOException, InterruptedException {
      List<KeyValue> kvList = new LinkedList<KeyValue>();

      for (KeyValue kv : values) {
        kvList.add(kv);
      }

      Collections.sort(kvList, KeyValue.COMPARATOR);
      for (KeyValue kv : kvList) {
        context.write(key, kv);
      }
    }
  }

  public static void main(String[] args)
      throws Exception {

    Configuration conf = HBaseConfiguration.create();

    // Compress Map output
    // conf.setBoolean("mapred.compress.map.output", true);
    // conf.setClass("mapred.map.output.compression.codec",
    // SnappyCodec.class, CompressionCodec.class);

    Job job = new Job(conf, "hb_psamxy");

    HTable table = new HTable(conf, "rtmml");

    job.setJarByClass(HBPsamXY.class);
    job.setInputFormatClass(AvroKeyInputFormat.class);
    // AvroJob.setInputKeySchema(job, DW_V_FACT_TRANS.SCHEMA$);
    job.setMapperClass(HBPsamXYMapper.class);
    job.setReducerClass(HBPsamXYReducer.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(KeyValue.class);

    HFileOutputFormat.configureIncrementalLoad(job, table);

    Path indir = new Path(args[0]);
    Path outdir = new Path(args[1]);

    FileInputFormat.addInputPath(job, indir);
    FileOutputFormat.setOutputPath(job, outdir);

    FileSystem fs = FileSystem.get(conf);
    fs.delete(outdir, true);

    SchemaMetrics.configureGlobally(conf);

    job.waitForCompletion(true);

    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
    loader.doBulkLoad(outdir, table);
  }
}
