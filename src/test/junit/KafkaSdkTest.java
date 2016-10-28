import com.yecredit.kafka.consumer.YscreditKafkaConsumer;
import com.yecredit.kafka.producer.YscreditKafkaProducerFactory;
import com.yecredit.kafka.sender.KafkaSender;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;

/**
 * Created by lizhengxian on 2016/10/27 0027.
 */
public class KafkaSdkTest {

    @Test
    public void testProducer(){
        //String server = "10.1.1.83:9092,10.1.1.84:9092,10.1.1.85:9092";
        String server = "10.1.1.25:9092";
        String topic = "test";
        String message = "hello world";
        YscreditKafkaProducerFactory producerFactory = new YscreditKafkaProducerFactory(server);
        //创建producer
        KafkaProducer producer = producerFactory.createProducer();

        //发送数据
        producer.send(new ProducerRecord(topic, message), new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if(null!=exception){
                    System.out.println("[发送失败]"+exception.getMessage());
                }else{
                    System.out.println("[发送成功]"+metadata.toString());
                }
            }
        });

        //关闭数据流
        producerFactory.closeProducer(producer);
    }

    @Test
    public void testConsumer(){
        String topics = "test,test2,test3";
        String kafkaServer = "localhost:9092";
        YscreditKafkaConsumer ys = new YscreditKafkaConsumer(topics,kafkaServer);
        ys.start();
    }

    @Test
    public void testKafkaSender(){
    }
}
