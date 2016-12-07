package com.ryxc.stock.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import com.ryxc.stock.bolt.StockFilterBolt;
import com.ryxc.stock.utils.EventScheme;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 * Created by tonye0115 on 2016/12/7.
 */
public class StockStategyTopology {
    public static void main(String[] args) throws Exception {
        //Configure kafka
        String zks = "10.9.12.10:2181";
        String topic = "stock";
        //default zookeeper root configuration for storm
        String zkRoot = "/kafkaStorm";
        String groupId = "kafkaSpout"; //groupId
        ZkHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, groupId);
        //将json传转为对象
        spoutConfig.scheme = new SchemeAsMultiScheme(new EventScheme());
        //该Topology因故障停止处理，下次正常运行时是否从Spout对应数据源Kafka中的该订阅Topic的起始位置开始读取
        spoutConfig.forceFromStart = false;

        //创建topology生成器
        TopologyBuilder builder = new TopologyBuilder();

        //Kafka里创建了一个3分区的Topic，这里并行度设置为3,设置了6个task任务(1个exectors线程启动2个task任务)
        builder.setSpout("kafka-reader",new KafkaSpout(spoutConfig),3).setNumTasks(6);
        // 设置数据处理节点名称，实例，并行度。
        builder.setBolt("stock-filter", new StockFilterBolt(), 2)//设置2个并行度（executor）
                .setNumTasks(2)//设置关联task个数
                .shuffleGrouping("kafka-reader");

        Config config = new Config();
        //设置一个spout task上面最多可以多少个没有处理的tuple，以防止tuple队列爆掉
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 10000);
        //config.setDebug(false);

        //分配几个进程来运行这个这个topology，建议大于物理机器数量。
        config.setNumWorkers(2);
        String name = StockStategyTopology.class.getSimpleName();
        StormTopology topology = builder.createTopology ();

        if (args != null && args.length > 0) {
            // Nimbus host name passed from command line
            config.put(Config.NIMBUS_HOST, args[0]);
            StormSubmitter.submitTopologyWithProgressBar(name, config, topology);
        } else {
            // 这里是本地模式下运行的启动代码。
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", config, topology);
            Thread.sleep(Integer.MAX_VALUE);
            cluster.shutdown();
        }



    }
}