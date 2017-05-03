package ind.diavolo.demo.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class P {
	public static void main(String[] args) {
		producer();
	}

	public final static String ip = "193.168.1.115:9092";

	public static void producer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", ip);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 1);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 100; i++){
			producer.send(new ProducerRecord<>("test",Integer.toString(i), "value"+Integer.toString(i)),new Callback() {
				
				@Override
				public void onCompletion(RecordMetadata arg0, Exception arg1) {
					System.out.printf("%s,%s\n", arg0.topic(),arg1);
				}
			});
			System.out.println(i);
		}
		producer.close();
	}
}
