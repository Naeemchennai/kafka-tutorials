import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.omg.SendingContext.RunTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThread {
    static Logger log=LoggerFactory.getLogger(ConsumerDemoThread.class);
    public static void main(String[] args) {
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"my-first-application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        CountDownLatch countDownLatch=new CountDownLatch(1);
        ConsumerThread consumerThread=new ConsumerThread(countDownLatch, "first_topic", properties);
        new Thread(consumerThread).start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            log.info("Caught shutdown hook");
            consumerThread.shutdown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error("Exception",e);
        }finally {
            log.info("Application is closing ");
        }
    }
}

class ConsumerThread implements Runnable {
    Logger logger= LoggerFactory.getLogger(ConsumerThread.class);
    private CountDownLatch countDownLatch;
    KafkaConsumer<String,String> kafkaConsumer;

    public ConsumerThread(CountDownLatch countDownLatch,
                          String topic,
                          Properties properties){

        kafkaConsumer=new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Collections.singleton(topic));
        this.countDownLatch=countDownLatch;

    }

    @Override
    public void run() {

        try {
        while (true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record:records){
                logger.info("{},{},{},{}",record.value(),record.topic(),record.partition(),record.timestamp());
            }
        }}catch (WakeupException e){
            logger.info("Recieved Shutdown signal!");
        }finally {
            kafkaConsumer.close();
            countDownLatch.countDown();
        }

    }
    public void shutdown(){
        kafkaConsumer.wakeup();
    }
}