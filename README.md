# Kafka SDK开发包

---------------------------------------
## Quick Start

进入项目路径，复制jar包，导入至自己的项目中。

```
ys_kafka_sdk/out/artifacts/kafkaSDK_jar/kafkaSDK.jar
```

## 发送一条数据
```java
import YscreditKafkaProducerFactory;

//初始化工厂
String topic = "test";//这个topic不能有逗号,发送数据必须指定单个topic
String message = "hello world";
String server = "localhost:9092";
YscreditKafkaProducerFactory producerFactory = new YscreditKafkaProducerFactory(server);

//配置一些属性(可选)
//producerFactory.setProperty("zookeeper.session.timeout.ms","400000");

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
```


## 发送多条数据(异步)
开启多个线程发送数据将显著改善发送速度

同样发送100条`"hello world"`

开启2个线程耗时大约`11004毫秒`

开启4个线程耗时大约`6004毫秒`

但也有性能瓶颈，取决于网络状况和Kafka的节点数量**(待验证)**

局域网单节点Kafka

开启30个进程，发送100条数据，耗时`922毫秒`

开启40个进程，发送100条数据，耗时`730毫秒`

开启50个进程，发送100条数据，耗时`555毫秒`

开启60个进程，发送100条数据，耗时`539毫秒`

```java
long start = System.currentTimeMillis();
String server = "localhost:9092";
String topic = "test1";

//将要发送的数据放入KafkaSender的集合中
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

while (true){
    if(KafkaSender.senderListIsNull()){
        long end = System.currentTimeMillis();
        System.out.println("总耗时:"+(end-start)+"毫秒");
        break;
    }
    sleep(1000);
}
```


## 接收数据
```java
import YscreditKafkaConsumer;

//这个topics可以有逗号，相当于监听多个topic
String topics = "test,test2,test3";
String kafkaServer = "localhost:9092";
YscreditKafkaConsumer consumer = new YscreditKafkaConsumer(topics,kafkaServer);
consumer.start();
```



## 配置
* 可以使用setProperty(String key,String value)或者loadProperties(Properties prop)更改或者加载自己的配置。
* YscreditKafkaProducerFactory类和YscreditKafkaConsumer类都有提供类似方法。
* 具体配置内容请查看 [官方文档](http://kafka.apache.org/documentation#producerconfigs "Apache Kafka")。

```java
YscreditKafkaProducerFactory producer = new YscreditKafkaProducerFactory("localhost:9092");
producer.setProperty("bootstrap.servers","localhost:9093");
//或者
Properties properties = new Properties();
properties.setProperty("bootstrap.servers","localhost:9093");
properties.setProperty("zookeeper.session.timeout.ms","400000");
producer.loadProperties(properties);
```