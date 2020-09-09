package com.huawei.demo.kafka;


import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;


import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.KafkaThread;


public class KafkaTransactionDemo {
	private static final long POLL_TIMEOUT_MS = 200;
	//change BOOTSTRAP_SERVERS
	private static final String BOOTSTRAP_SERVERS="192.168.2.114:9092,192.168.2.210:9092,192.168.2.157:9092";
	//change CONSUME_TOPIC
	private static final String CONSUME_TOPIC = "topic-type";
	private static final String PRODUCE_TOPIC = "topic-type";
	private static final String MESSAGE = "transcation-mess-data-";
	 
    public static void main(String[] args) {
    	
    	String operation = System.getProperty("operation");
    	
    	new KafkaTransactionDemo().onlyProducerInTransaction(operation);
    }

    /**
     * 在一个事务只有生产消息操作
     */
    public void onlyProducerInTransaction(String operation) {
        Producer producer = buildProducer();
        // 1.初始化事务
        producer.initTransactions();
        
        // 2.开启事务
        producer.beginTransaction();
        try {
            // 3.kafka写操作集合
            // 3.1 do业务逻辑
            // 3.2 发送消息

        	for (int i=0; i< 20;i++) {
            producer.send(new ProducerRecord<String, String>(CONSUME_TOPIC,MESSAGE + i));
            System.out.println("Message:" + MESSAGE + i +" send!");
            Thread.sleep(1000);
            // 3.3 do其他业务逻辑,还可以发送其他topic的消息。
        	}
            // 4.事务提交
        	if(operation.equals("commit")) {
        		producer.commitTransaction();
        		System.out.println("Tracnscation commit success!");
        	}else {
        		producer.abortTransaction();
        		System.out.println("Tracnscation abort!");
        	}
 	
        } catch (Exception e) {
        	System.out.printf("Transaction failed, rollback, cause is: %s\n", e.getMessage());
            // 5.放弃事务
            producer.abortTransaction();
        }
    }

    /**
//     * 创建一个事务，在这个事务操作中，只有生成消息操作，如下代码。
//     * 这种操作其实没有什么意义，跟使用手动提交效果一样，无法保证消费消息操作和提交偏移量操作在一个事务
//     */
//    public void onlyConsumerInTransaction() {
//        // 1.构建上产者
//        Producer producer = buildProducer();
//        // 2.初始化事务(生成productId),对于一个生产者,只能执行一次初始化事务操作
//        producer.initTransactions();
//        // 3.构建消费者和订阅主题
//        Consumer consumer = buildConsumer();
//        consumer.subscribe(Arrays.asList(CONSUME_TOPIC));
//
//        while (true) {
//            // 4.开启事务
//            producer.beginTransaction();
//            // 5.1 接受消息
//            ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT_MS);
//
//            try {
//                // 5.2 do业务逻辑;
//                System.out.println("customer Message---");
//                Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
//
//                for (ConsumerRecord<String, String> record : records) {
//                    // 5.2.1 读取消息,并处理消息。print the offset,key and value for the consumer records.
//                    // 此处为真正的处理消息，与步骤7的提交偏移量不在一个事务里面
//                    System.out.printf(Thread.currentThread().getName() + ": partition = %d, offset = %d, key = %s, value = %s, timestamp = %s,timestampType = %s %n", record.partition(), record.offset(), record.key(), record.value(), record.timestamp(), record.timestampType());
//
//                    // 5.2.2 记录提交的偏移量
//                    commits.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
//                }
//                // 7.提交偏移量
//                producer.sendOffsetsToTransaction(commits, "test_group0111");
//
//                // 8.事务提交
//                producer.commitTransaction();
//            } catch (Exception e) {
//                // 7.放弃事务
//                producer.abortTransaction();
//            }
//        }
//    }
//    /**
//     * 消费-生产并存（consume-transform-produce）
//     * 在一个事务中，既有生产消息操作又有消费消息操作，即常说的Consume-tansform-produce模式
//     * 这是kafka事务最能实现价值的方式
//     */
//    public void consumerTransformProducer() {
//        // 1.构建上产者
//        Producer producer = buildProducer();
//        // 2.初始化事务(生成productId),对于一个生产者,只能执行一次初始化事务操作
//        producer.initTransactions();
//
//        // 3.构建消费者和订阅主题
//        Consumer consumer = buildConsumer();
//        consumer.subscribe(Arrays.asList(CONSUME_TOPIC));
//
//        while (true) {
//            // 4.开启事务
//            producer.beginTransaction();
//            // 5.1 接受消息
//            ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT_MS);
//
//            try {
//                // 5.2 do业务逻辑;
//                System.out.println("customer Message---");
//                Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
//
//                for (ConsumerRecord<String, String> record : records) {
//                    // 5.2.1 打印消息。print the offset,key and value for the consumer records.
//                    System.out.printf(Thread.currentThread().getName() + ": partition = %d, offset = %d, key = %s, value = %s, timestamp = %s,timestampType = %s %n", record.partition(), record.offset(), record.key(), record.value(), record.timestamp(), record.timestampType());
//
//                    // 5.2.2 记录提交的偏移量
//                    commits.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
//
//                    // 6.生产新的消息。比如外卖订单状态的消息,如果订单成功,则需要发送跟商家结转消息或者派送员的提成消息
//                    // 此处为真正的处理消息，与步骤7的提交偏移量在一个事务里面
//                    producer.send(new ProducerRecord<String, String>(PRODUCE_TOPIC, "tran-" + record.value()));
//
//                }
//                // 7.提交偏移量
//                producer.sendOffsetsToTransaction(commits, "test_group0111");
//
//                // 8.事务提交，同时成功
//                producer.commitTransaction();
//                System.out.println("Tracnscation commit success!");
//            } catch (Exception e) {
//                // 7.放弃事务，同时失败
//            	System.out.printf("Transaction failed, rollback, cause is: %s\n", e.getMessage());
//                producer.abortTransaction();
//            }
//        }
//    }
//    /**
//     * 幂等性测试,无事务
//     */
//    public void consumerTransformProducerByIdempotence() {
//        // 1.构建上产者
//        Producer producer = buildProducer();
//
//        // 3.构建消费者和订阅主题
//        Consumer consumer = buildConsumer();
//        consumer.subscribe(Arrays.asList(CONSUME_TOPIC));
//
//        while (true) {
//            // 5.1 接受消息
//            ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT_MS);
//
//            try {
//                // 5.2 do业务逻辑;
//                System.out.println("Processing Message ---");
//                Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
//
//                for (ConsumerRecord<String, String> record : records) {
//                    // 5.2.1 读取消息,并处理消息。print the offset,key and value for the consumer records.
//                    System.out.printf(Thread.currentThread().getName() + ": partition = %d, offset = %d, key = %s, value = %s, timestamp = %s,timestampType = %s %n", record.partition(), record.offset(), record.key(), record.value(), record.timestamp(), record.timestampType());
//
//                    // 5.2.2 记录提交的偏移量
//                    commits.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
//
//                    // 6.生产新的消息。比如外卖订单状态的消息,如果订单成功,则需要发送跟商家结转消息或者派送员的提成消息
//                    producer.send(new ProducerRecord<String, String>("test.2", "idem-" + record.value()));
//
//                }
//                // 7.提交偏移量，不能做到一个事务里面，不能做到同时成功和同时失败
//                consumer.commitSync();
//
//            } catch (Exception e) {
//            }
//        }
//    }

    /**
     * 需要:
     * 1、设置transactional.id
     * 2、设置enable.idempotence
     *
     * @return
     */

    private Producer buildProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("client.id", "producer_01");
//        props.put("acks", "all");
        props.put("enable.idempotence", true); // 设置了幂等性为true则会自动将acks设置成all，如果强制设置成非all，则会报错
        props.put("transactional.id", "tran1"); //设置了事务 幂等性自动设置为true，如果强制设置成非true则会报错
        props.put("retries", 3);
        props.put("max.in.flight.requests.per.connection", 5);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("compression.type", "gzip");//none, gzip, snappy, 或者 lz4. 默认none
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      //  props.put("partitioner.class", "com.kafka.CustomPartitioner");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }

    /**
     * 需要:
     * 1、关闭自动提交 enable.auto.commit 设置为 false，会在下游producer完成以后，commit offset
     * 2、isolation.level为 read_committed, 主要是为了处理上游的producer是事务型的提交
     *
     * @return
     */

    public Consumer buildConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("group.id", "test_group0111");
        props.put("client.id", "consumer_01");
        // 设置隔离级别
        props.put("isolation.level", "read_committed");
        // 关闭自动提交
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("max.poll.interval.ms", "300000");
        // 在没有offset的情况下采取的拉取策略，[latest, earliest, none]
        props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//            props.put("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        return consumer;
    }

}


