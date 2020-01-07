package sentyppe;

import config.BusiConst;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import util.ProducerUtils;


/**
 * kafka发送消息异步回调通知
 */
public class KafkaAsynProducer {

    public static void main(String[] args) {
        KafkaProducer<String,String> kafkaProducer =  ProducerUtils.getKafkaProducer();
        ProducerRecord producerRecord = new ProducerRecord(BusiConst.HELLO_TOPIC, "teacher", "mark");
        try{
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e != null){
                        e.printStackTrace();
                    }
                    if(recordMetadata != null){
                        System.out.println(String.format("topic : %s; offset : %d; partition : %d", recordMetadata.topic(), recordMetadata.offset(), recordMetadata.partition()));
                    }
                }
            });
        }finally {
            kafkaProducer.close();
        }
    }
}
