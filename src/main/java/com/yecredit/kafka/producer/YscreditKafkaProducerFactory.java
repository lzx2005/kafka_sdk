package com.yecredit.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

// TODO: 2016/10/27 0027 编写可以异步且不会内存溢出的方法

/**
 * @author lizhengxian
 * @date 2016.10.27
 * @version 1.0.0
 */
public class YscreditKafkaProducerFactory {

    /**
     * 全局配置文件，可以使用setProperties()方法更新或者loadProperties替换
     */
    private Properties properties;


    /**
     * 实例化一个YscreditKafkaProducer
     */
    public YscreditKafkaProducerFactory(String kafkaServers){
        super();
        this.properties = new Properties();
        InputStream ins = Object.class.getResourceAsStream("/defaultKafkaProducer.properties");
        try {
            properties.load(ins);
        } catch (IOException e) {
            System.err.println("初始化YscreditKafkaProducer失败:"+e.getMessage());
            e.printStackTrace();
        }
        properties.setProperty("bootstrap.servers",kafkaServers);
        properties.setProperty("metadata.broker.list",kafkaServers);
    }


    /**
     * 修改properties的内容，详细内容请参考http://kafka.apache.org/documentation#producerconfigs
     * @param key       配置的key值
     * @param value     配置的value值
     */
    public void setProperty(String key,String value){
        properties.put(key,value);
    }

    /**
     * 替换自己的properties
     * @param properties    一个properties实例，可以new Properties()，也可以从文件中读取
     */
    public void loadProperties(Properties properties){
        this.properties = properties;
    }


    /**
     * 根据properties生成一个producer
     * @return
     */
    public KafkaProducer createProducer() {
        return new KafkaProducer<String,String>(properties);
    }

    public KafkaProducer getProducer(){
        return createProducer();
    }


    /**
     * 关闭producer，重置producer
     */
    public void closeProducer(Producer producer){
        producer.close();
        producer.flush();
    }


    /**
     * 示例代码
     * @param args
     */
    public static void main(String[] args) {
        //初始化工厂
        String topic = "test1";
        String message = "hello world";
        String server = "10.1.1.25:9092";
        YscreditKafkaProducerFactory producerFactory = new YscreditKafkaProducerFactory(server);

        //配置一些属性
        producerFactory.setProperty("zookeeper.session.timeout.ms","400000");

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

}