package ind.diavolo.demo.storm;

import java.util.Arrays;
import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class T1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public static void f() throws Exception {
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("the cow jumped over the moon"), new Values("the man went to the store and bought some candy"), new Values("four score and seven years ago"),
				new Values("how many apples can you eat"));
		spout.setCycle(true);
		TridentTopology topology = new TridentTopology();
		TridentState wordCounts = topology.newStream("spout1", spout).each(new Fields("sentence"), new Split(), new Fields("word")).groupBy(new Fields("word"))
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count")).parallelismHint(6);
	}

	public static void f2() throws Exception {
		String zks = "193.168.1.115:2181";
		String topic = "his_dev";
		String zkRoot = "/storm"; // default zookeeper root configuration for storm
		String id = "word";
		BrokerHosts brokerHosts = new ZkHosts(zks);
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConf.zkServers = Arrays.asList(new String[] { "h1", "h2", "h3" });
		spoutConf.zkPort = 2181;
		KafkaSpout ks = new KafkaSpout(spoutConf);

		KafkaBolt kb = new KafkaBolt().withTopicSelector(new DefaultTopicSelector(topic)).withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("ks", new KafkaSpout(spoutConf), 5); // Kafka我们创建了一个5分区的Topic，这里并行度设置为5
		builder.setBolt("b1", new B1(), 2).shuffleGrouping("ks");
		builder.setBolt("kb", kb);
		Config conf = new Config();
		conf.put(Config.NIMBUS_SEEDS, "");
		conf.setNumWorkers(1);
		conf.put(Config.TOPOLOGY_DEBUG, true);
		conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
		Properties props = new Properties();
	    props.put("metadata.broker.list", "localhost:9092");
	    props.put("request.required.acks", "1");
	    props.put("serializer.class", "kafka.serializer.StringEncoder");
//	    conf.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
		StormSubmitter.submitTopology("T1", conf, builder.createTopology());
	}
}
