package selfserial;

import config.BusiConst;
import config.KafkaConst;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import vo.DemoUser;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SelfSeraliProduce {
    private static KafkaProducer<String, DemoUser> kafkaProducer;

    public static void main(String[] args) {
        kafkaProducer = new KafkaProducer<String, DemoUser>(KafkaConst.producerConfig(StringSerializer.class, SelfSerializer.class));
        ProducerRecord producerRecord = new ProducerRecord(BusiConst.HELLO_TOPIC1, "张三", new DemoUser(1, "张恒"));
        Future<RecordMetadata> future = kafkaProducer.send(producerRecord);
        try {
            RecordMetadata recordMetadata = future.get();
            System.out.println(recordMetadata);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }finally {
            kafkaProducer.close();
        }
    }
}
