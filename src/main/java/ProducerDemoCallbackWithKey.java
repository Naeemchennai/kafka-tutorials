import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoCallbackWithKey {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger= LoggerFactory.getLogger(ProducerDemoCallbackWithKey.class);
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {
            String key="id_"+i;
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", key ,"HellowRold"+ i);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e==null) {
                        logger.info("Metadata Topic" + recordMetadata.topic()
                                + "\n Offset" + recordMetadata.offset()
                                + "\n Partition" + recordMetadata.partition()
                                + "\n TimeStamp" + recordMetadata.timestamp());
                    }else {
                        logger.error("Exception while processing",e);
                    }
                }
            }).get();
        }
        producer.flush();
        producer.close();
    }
}
