package api.kafka;

import api.kafka.utils.KafkaUtils;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class ConsumerV1_0  implements IConsumer{
    Properties props;

    KafkaConsumer<String,String> consumer;

    ConcurrentHashMap<String, LinkedBlockingQueue<String>> events;


    public ConsumerV1_0(String schemaRegistryUrl,List<String> bootstrapServers,List<String>topics) {
        props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaUtils.listToString(bootstrapServers));
        props.put("schema.registry.url",schemaRegistryUrl);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        consumer = new KafkaConsumer<>(props);
        events = new ConcurrentHashMap<>();
        consumer.subscribe(topics);
    }
    public void connect(){

        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
            if(records!=null && !records.isEmpty()){

            }
        }


    }



}
