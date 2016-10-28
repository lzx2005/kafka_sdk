package com.lzx2005.kafka.sender;

import com.lzx2005.kafka.producer.KafkaProducerFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.LinkedList;

/**
 * @author lizhengxian
 * @date 2016.10.27
 * @version 1.0.0
 */
public class KafkaSender extends Thread{

    /**
     *
     */
    private static LinkedList<String> messageList=new LinkedList<String>();
    private String servers;
    private String topic;
    private boolean isRunning = false;

    /**
     * 创建一个KafkaSender
     * @param servers   Kafka地址
     * @param topic     Kafka的topic
     */
    public KafkaSender(String servers,String topic) {
        this.servers = servers;
        this.topic = topic;
        this.isRunning = true;
    }


    public void run() {
        while (isRunning){
            final String firstMessage = getMessage();
            if(firstMessage!=null){
                System.out.println("线程["+Thread.currentThread().getId()+"]发送数据["+firstMessage+"]到Kafka，正在等待响应...");
                KafkaProducerFactory producerFactory = new KafkaProducerFactory(servers);
                KafkaProducer producer = producerFactory.createProducer();
                producer.send(new ProducerRecord(topic, firstMessage), new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(null!=exception){
                            System.out.println("[发送失败]"+exception.getMessage());
                            KafkaSender.putMessage(firstMessage);
                        }else{
                            System.out.println("['"+firstMessage+"'发送成功]"+metadata.toString());
                        }
                    }
                });
                producer.close();
            }else{
                System.out.println("任务完成，关闭数据发送进程:["+Thread.currentThread().getId()+"]"+Thread.currentThread().getName());
                break;
            }
        }
    }

    /**
     * 获得一个Message
     * @return
     */
    private synchronized String getMessage(){
        return messageList.poll();
    }

    /**
     * 外部调用，发送Kafka
     * @param message
     */
    public static void putMessage(String message){
        messageList.add(message);
        System.out.println("消息["+message+"]已加入队列");
    }


    /**
     * 是否发送完毕
     * @return
     */
    public static boolean senderListIsNull(){
        return messageList.size()==0;
    }

    /**
     * 关闭进程
     */
    public void shutdown(){
        this.isRunning = false;
    }

    public static void main(String[] args) throws InterruptedException {
        long start = System.currentTimeMillis();
        String server = "localhost:9092";
        String topic = "test1";

        //将要发送的数据放入KafkaSender
        for (int i=0;i<100;i++){
            KafkaSender.putMessage("hello world , "+i);
        }

        //开启四个进程发送数据
        KafkaSender sender1 = new KafkaSender(server,topic);
        sender1.start();
        KafkaSender sender2 = new KafkaSender(server,topic);
        sender2.start();
        KafkaSender sender3 = new KafkaSender(server,topic);
        sender3.start();
        KafkaSender sender4 = new KafkaSender(server,topic);
        sender4.start();

        for(int i=0;i<26;i++){
            new KafkaSender(server,topic).start();
        }

        while (true){
            if(KafkaSender.senderListIsNull()){
                long end = System.currentTimeMillis();
                sleep(1000);
                System.err.println("总耗时:"+(end-start)+"毫秒");
                break;
            }
            sleep(10);
        }
    }
}
