package com.yecredit.kafka.consumer;

import org.apache.kafka.clients.consumer.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author lizhengxian
 * @date 2016.10.27
 * @version 1.0.0
 */
public class YscreditKafkaConsumer extends Thread{

    private KafkaConsumer<String,String> consumer;
    private ArrayList<String> topics;
    private Properties properties;

    /**
     * 初始化YscreditKafkaConsumer
     * @param topicStr      topic的列表，以逗号','分割，例如:"topic1,topic2,topic3"
     * @param kafkaServers  kafka服务器的地址，也可以以逗号','分割，例如:"localhost:9091,localhost:9092,localhost:9093"
     */
    public YscreditKafkaConsumer(String topicStr,String kafkaServers){
        super();
        properties = new Properties();
        //加载默认配置
        InputStream ins = Object.class.getResourceAsStream("/defaultKafkaConsumer.properties");
        try {
            properties.load(ins);
        } catch (IOException e) {
            System.err.println("初始化YscreditKafkaConsumer失败:"+e.getMessage());
            e.printStackTrace();
        }

        //加载服务器
        properties.setProperty("bootstrap.servers",kafkaServers);

        //加载topics
        String[] topicsArray = topicStr.split(",");
        ArrayList<String> topicsList = new ArrayList<String>();
        topicsList.addAll(Arrays.asList(topicsArray));
        this.topics = topicsList;

        //创建consumer
        this.consumer = createConsumer();
        if(consumer!=null){
            System.out.println("consumer创建成功！");
        }
    }


    /**
     * 修改properties的内容，详细内容请参考http://kafka.apache.org/documentation#consumerconfigs
     * @param key       配置的key值
     * @param value     配置的value值
     */
    public void setProperties(String key,String value){
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
     * 开启一个线程监听kafka
     */
    @Override
    public void run() {
        super.run();
        System.out.println("开始监听：");
        while (true){
            ConsumerRecords<String,String> results  = consumer.poll(100);
            for (ConsumerRecord<String, String> record : results)
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
        }
    }

    /**
     * 获取一个KafkaConsumer实例
     * @return
     */
    private KafkaConsumer<String,String> createConsumer(){
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(topics);
        return kafkaConsumer;
    }

    /**
     * 示例代码
     * @param args
     */
    public static void main(String[] args){
        String topics = "test1";
        String kafkaServer = "10.1.1.25:9092";
        YscreditKafkaConsumer ykc = new YscreditKafkaConsumer(topics,kafkaServer);
        ykc.start();
    }

}