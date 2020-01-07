package hellokafka;

import config.BusiConst;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 简单kafka消费者
 */
public class HelloKafkaConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "129.211.51.230:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        KafkaConsumer<String,String> kafkaConsumer = null;
        try {
            kafkaConsumer = new KafkaConsumer(properties);
            //绑定当前主题
            kafkaConsumer.subscribe(Collections.singleton(BusiConst.HELLO_TOPIC));
            while(true){
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1));
                for(ConsumerRecord consumerRecord : consumerRecords){
                    System.out.println(String.format("topic: %s, 分区 : %d, 偏移量： %d, ket:%s, value : %s", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), consumerRecord.value()));
                }
            }
        }finally {
            kafkaConsumer.close();
        }

    }

}
