import com.lzx2005.kafka.consumer.KafkaConsumer;
import com.lzx2005.kafka.producer.KafkaProducerFactory;
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
        String server = "localhost:9092";
        String topic = "test";
        String message = "hello world";
        KafkaProducerFactory producerFactory = new KafkaProducerFactory(server);
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
        KafkaConsumer ys = new KafkaConsumer(topics,kafkaServer);
        ys.start();
    }

    @Test
    public void testKafkaSender(){
    }
}
