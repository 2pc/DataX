package com.alibaba.datax.plugin.writer.kafkawriter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by luping on 2017/11/15.
 */
public class KafkaProducerTest {


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.28.3.158:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        try{
            for (int i =0; i<10;i++){
                producer.send(new ProducerRecord<String, String>("test5pppp", String.valueOf(i), "sssss")).get();
            }

            System.out.println("end");
        }catch(Exception e){
            System.out.println("ccccc");
        }
    }

}
