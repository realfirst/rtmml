package com.lakala.rtmml.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PsamXY extends Configured implements Tool {

  private static final String S_NO = "S_NO";

  private static final String LONGITUDE = "LONGITUDE";
  private static final String LATITUDE = "LATITUDE";

  private static final String PSAM = "PSAM";
  private static final String TERM_TYPE = "TERM_TYPE";

  private static final Schema outputSchema;
  // private static final Schema dictSchema;

  static {
    try {
      outputSchema = new Schema.Parser()
                     .parse(PsamXY.class
                            .getResourceAsStream("output.schema"));
      // dictSchema =  new Schema.Parser()
                    // .parse(PsamXY.class
                           // .getResourceAsStream("TERM_TYPE.avsc"));
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static class PsamXYMapper
      extends
      Mapper<AvroKey<GenericRecord>, NullWritable, Text, AvroValue<GenericRecord>> {

    private Set<String> typeSet = new HashSet<String>();

    /**
     * Describe <code>setup</code> method here.
     *
     * @param context a <code>Mapper.Context</code> value
     * @exception IOException if an error occurs
     * @exception InterruptedException if an error occurs
     */
    @Override
    public final void setup(Context context)
        throws IOException, InterruptedException {
      List<String> list = new ArrayList<String>(Arrays.asList("M280P(W)", "M280P(E)", "M960E(W)", "M960E(E)", "M280P(WT)", "M280P(ET)", "M960E(WT)", "M960E-IP-W", "M960E-2G-W", "M960E-IP-E", "M960E-2G-E", "M280P-2G-W", "M280P-2G-E", "LDA"));
      typeSet.addAll(list);
      /*
        Path[] localPaths = DistributedCache
        .getLocalCacheFiles(context.getConfiguration());
        if (localPaths.length == 0) {
        throw new FileNotFoundException("distributed cache not found!");
        }

        File termDictFile = new File(localPaths[0].toString());

        DatumReader<GenericRecord> datumReader =
        new GenericDatumReader<GenericRecord>(dictSchema);
        DataFileReader<GenericRecord> dataFileReader =
        new DataFileReader<GenericRecord>(termDictFile, datumReader);
        GenericRecord type = null;
        while (dataFileReader.hasNext()) {
        type = dataFileReader.next(type);
        typeSet.add((String)type.get("CODE"));
        }
      */
    }

    /**
     * Describe <code>map</code> method here.
     *
     * @param object an <code>Object</code> value
     * @param object1 an <code>Object</code> value
     * @param context a <code>Mapper.Context</code> value
     * @exception IOException if an error occurs
     * @exception InterruptedException if an error occurs
     */
    @Override
    public final void map(AvroKey<GenericRecord> key,
                          NullWritable value,
                          Context context)
        throws IOException, InterruptedException {
      GenericRecord data = key.datum();
      GenericRecord output = new GenericData.Record(outputSchema);
      // CharSequence s_no = (CharSequence)data.get(S_NO);
      // Log.info("s_no:" + data.get(S_NO));
      output.put(S_NO, data.get(S_NO));
      if (data.get(PSAM) == null) {
        if (data.get(LATITUDE) != null && data.get(LONGITUDE) != null) {
          String[] parts = ((String)data.get(LATITUDE)).split("\\|");
          for (int i = 0; i < parts.length; i++) {
            System.out.println("parts[" + i + "]:" + parts[i]);
          }

          output.put(LATITUDE, parts[0].replaceFirst("^0+(?!$)", "") + "." + parts[1] + (parts[2].replace(".", "")));
          String[] parts1 = ((String)data.get(LONGITUDE)).split("\\|");
          // parts1 = ((String)data.get(LONGITUDE)).split("|");
          output.put(LONGITUDE, parts1[0].replaceFirst("^0+(?!$)", "") + "." + parts1[1] + (parts1[2].replace(".", "")));
          context.write(new Text(data.get(S_NO).toString()), new AvroValue<GenericRecord>(output));
        }
      } else {
        if (typeSet.contains((data.get(TERM_TYPE)))) {
          output.put(PSAM, data.get(PSAM));
          output.put(TERM_TYPE, data.get(TERM_TYPE));
          context.write(new Text(data.get(S_NO).toString()), new AvroValue<GenericRecord>(output));
        }
      }
    }
  }

  public static class PsamXYReducer
      extends Reducer<Text, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> {

    /**
     * Describe <code>reduce</code> method here.
     *
     * @param object an <code>Object</code> value
     * @param iterable an <code>Iterable</code> value
     * @param context a <code>Reducer.Context</code> value
     * @exception IOException if an error occurs
     * @exception InterruptedException if an error occurs
     */
    @Override
    public final void reduce(Text key, Iterable<AvroValue<GenericRecord>> values,  Context context)
        throws IOException, InterruptedException {
      GenericRecord output = new GenericData.Record(outputSchema);

      for (AvroValue<GenericRecord> value : values) {
        GenericRecord datum = value.datum();
        for (Schema.Field field : datum.getSchema().getFields()) {
          String fieldName = field.name();
          Object fieldValue = datum.get(fieldName);
          if (fieldValue != null) {
            output.put(fieldName, fieldValue);
          }
        }
      }

      CharSequence psam = (CharSequence)output.get(PSAM);
      CharSequence longitude = (CharSequence)output.get(LONGITUDE);
      CharSequence latitude = (CharSequence)output.get(LATITUDE);
      if (psam != null && longitude != null && latitude != null) {
        context.write(new AvroKey<GenericRecord>(output), NullWritable.get());
      }
    }
  }

  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if (args.length != 2) {
      System.err.printf("Usage: %s <comma separated paths> <output path>\n", this.getClass().getName());
      return -1;
    }

    Job job = Job.getInstance();
    job.setJobName("PasmJoin");
    job.setJarByClass(PsamXY.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(AvroValue.class);
    job.setOutputKeyClass(AvroKey.class);
    job.setOutputValueClass(NullWritable.class);

    job.setMapperClass(PsamXYMapper.class);
    job.setReducerClass(PsamXYReducer.class);

    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setOutputFormatClass(AvroKeyOutputFormat.class);

    FileInputFormat.setInputPaths(job, args[0]);
    Path output = new Path(args[1]);    
    FileOutputFormat.setOutputPath(job, output);
    FileSystem fs = FileSystem.get(conf);
    fs.delete(output, true);

    AvroJob.setOutputKeySchema(job, outputSchema);
    AvroJob.setMapOutputValueSchema(job, outputSchema);

    // DistributedCache.addCacheFile(new Path("BM_TERM_TYPE_DMT.avro").toUri(),
    // job.getConfiguration());

    job.setNumReduceTasks(1);
    job.submit();

    job.waitForCompletion(true);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new PsamXY(), args);
    System.exit(res);
  }

}
