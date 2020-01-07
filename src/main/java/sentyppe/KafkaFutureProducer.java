package sentyppe;

import config.BusiConst;
import config.KafkaConst;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * kafka同步发送数据
 */
public class KafkaFutureProducer {

    private static KafkaProducer<String,String> kafkaProducer;

    public static void main(String[] args) {
        kafkaProducer = new  KafkaProducer<String,String>(KafkaConst.producerConfig(StringSerializer.class, StringSerializer.class));
        ProducerRecord<String,String> producerRecord = new ProducerRecord(BusiConst.HELLO_TOPIC, "sadas", "特殊消息");
        Future<RecordMetadata> recordMetadataFuture = kafkaProducer.send(producerRecord);
        try {
            System.out.println(recordMetadataFuture.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }finally {
            kafkaProducer.close();
        }
    }
}
