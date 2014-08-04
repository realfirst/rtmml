package com.lakala.rtmml;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import com.lakala.rtmml.bolt.HBasePutBolt;

public class LogTopology {

  public static void main(String[] args) throws Exception {

    BrokerHosts zk = new ZkHosts("localhost");    
    SpoutConfig kafkaConf = new SpoutConfig(zk, "mmtlog", "/kafkastorm", "discovery");
    kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());

    KafkaSpout kafkaSpout = new KafkaSpout(kafkaConf);

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("spout", kafkaSpout, 2);
    builder.setBolt("hbase-put", new HBasePutBolt())
        .shuffleGrouping("spout");

    Config config = new Config();
    // config.put(Config.TOPOLOGY_DEBUG, true);
    config.setDebug(true);

    if(args!= null && args.length > 0) {
      config.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], config, builder.createTopology());
    } else {
      config.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("kafka", config, builder.createTopology());

      Thread.sleep(10000);

      cluster.shutdown();
    }

  }

}
