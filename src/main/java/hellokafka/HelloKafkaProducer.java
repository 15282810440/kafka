package hellokafka;

import config.BusiConst;
import config.KafkaConst;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 简单kafka生产者
 */
public class HelloKafkaProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "129.211.51.230:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> kafkaProducer = null;
        try{
            kafkaProducer = new KafkaProducer(properties);
            ProducerRecord producerRecord = null;
            try{
               for(int i = 0; i < 10000; i++){
                   producerRecord = new ProducerRecord(BusiConst.HELLO_TOPIC, "testTeacher2", "lison");
                   kafkaProducer.send(producerRecord);
               }
            }catch (Exception e){
                e.printStackTrace();
            }
            System.out.println("msg send");
        }finally {
            kafkaProducer.close();
        }
    }
}
