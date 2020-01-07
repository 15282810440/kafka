package selfserial;

import config.BusiConst;
import config.KafkaConst;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import vo.DemoUser;

import java.util.Collections;

public class SelfSeraliConsumer {

    private static KafkaConsumer kafkaConsumer;

    public static void main(String[] args) {
        kafkaConsumer = new KafkaConsumer<String, DemoUser>(KafkaConst.consumerConfig("testGroup", StringDeserializer.class, SelfDeSerializer.class));
        kafkaConsumer.subscribe(Collections.singleton(BusiConst.HELLO_TOPIC1));
        try
        {
            while(true){
                ConsumerRecords<String, DemoUser> consumerRecords = kafkaConsumer.poll(500);
                for(ConsumerRecord<String, DemoUser> record : consumerRecords){
                    System.out.println(String.format(
                            "主题：%s，分区：%d，偏移量：%d，key：%s，value：%s",
                            record.topic(),record.partition(),record.offset(),
                            record.key(),record.value()));
                }
            }
        }finally {
            kafkaConsumer.close();
        }
    }

}
