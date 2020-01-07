package commit;

import config.BusiConst;
import config.KafkaConst;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * x消费者消费异步提交
 */
public class CommitAsync {

    public static void main(String[] args) {
        Properties properties = KafkaConst.consumerConfig("test1",StringDeserializer.class, StringDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String,String> kafkaConsumer = null;
        try{
            kafkaConsumer = new KafkaConsumer<String,String>(properties);
            kafkaConsumer.subscribe(Collections.singleton(BusiConst.HELLO_TOPIC));
            while(true){
                ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1));
                for(ConsumerRecord consumerRecord : consumerRecords){
                    System.out.println(consumerRecord);
                }
                kafkaConsumer.commitAsync();
            }
        }finally {
            kafkaConsumer.close();
        }
    }


}
