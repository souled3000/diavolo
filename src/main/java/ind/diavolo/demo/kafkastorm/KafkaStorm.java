package ind.diavolo.demo.kafkastorm;

import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;

public class KafkaStorm {
    private String zkUrl;
    private String brokerUrl;

    KafkaStorm(String zkUrl, String brokerUrl) {
        this.zkUrl = zkUrl;
        this.brokerUrl = brokerUrl;
    }
    
    private TransactionalTridentKafkaSpout createKafkaSpout() {
        ZkHosts hosts = new ZkHosts(zkUrl);
        TridentKafkaConfig config = new TridentKafkaConfig(hosts, "test");
        config.scheme = new SchemeAsMultiScheme(new StringScheme());

        // Consume new data from the topic
        config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        return new TransactionalTridentKafkaSpout(config);
    }
    
    private TridentState addTridentState(TridentTopology tridentTopology) {
        return tridentTopology.newStream("spout1", createKafkaSpout()).parallelismHint(1)
                .each(new Fields("str"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(1);
    }

}
