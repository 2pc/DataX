package com.alibaba.datax.plugin.reader.kafkareader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


/**
 * Created by quchuanyuan on 2017/7/5.
 */
public class KafkaReader extends Reader {
    public static class Job extends Reader.Job {
        private Configuration originalConfiguration = null;
        @Override
        public void init() {
            this.originalConfiguration = super.getPluginJobConf();
        }
        @Override
        public void prepare() {
            super.prepare();
        }


        @Override
        public void post() {
            super.post();
        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> readerSplitConfiguration = new ArrayList<Configuration>();
            for (int i = 0; i < adviceNumber; i++) {
                readerSplitConfiguration.add(this.originalConfiguration);
            }
            return readerSplitConfiguration;
        }
    }

    public static class Task extends Reader.Task {


        private Configuration readerSliceConfiguration;

        private static String bootstrapServers;

        private static String topics;

        private static String keySerializer;

        private static String valueSerializer;

        private static String groupId;

        private static Boolean autoCommit;

        private static Integer pollIntervalTime;

        private KafkaConsumer<String,String> consumer;

        //钩子的标志
        private static volatile boolean isRun=true;

        @Override
        public void init() {
            this.readerSliceConfiguration=super.getPluginJobConf();
            this.bootstrapServers=this.readerSliceConfiguration.getString(Key.BOOTSTRAP_SERVERS,null);
            this.topics=this.readerSliceConfiguration.getString(Key.TOPICS,null);
            this.keySerializer = this.readerSliceConfiguration.getString(Key.KEY_SERIALIZER,null);
            this.valueSerializer= this.readerSliceConfiguration.getString(Key.VALUE_SERIALIZER,null);
            this.groupId=this.readerSliceConfiguration.getString(Key.GROUP_ID,null);
            this.autoCommit=this.readerSliceConfiguration.getBool(Key.AUTO_COMMIT,true);
            this.pollIntervalTime=this.readerSliceConfiguration.getInt(Key.POLL_INTERVAL_TIME,100);
        }

        @Override
        public void prepare() {
            Properties properties = new Properties();
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
            if (StringUtils.isEmpty(bootstrapServers)) {
                throw DataXException
                        .asDataXException(
                                KafkaReaderErrorCode.REQUIRED_VALUE,
                                String.format("请检查名称: [%s].",
                                        Key.BOOTSTRAP_SERVERS));
            }
            if (StringUtils.isNotEmpty(keySerializer)) {
                properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keySerializer);
            }
            if (StringUtils.isNotEmpty(valueSerializer)) {
                properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueSerializer);
            }
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,this.autoCommit);//手动提交
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,this.bootstrapServers);//指定broker地址，来找到group的coordinator
            properties.put(ConsumerConfig.GROUP_ID_CONFIG,this.groupId);//指定用户组
            this.consumer = new KafkaConsumer<String, String>(properties);
            this.consumer.subscribe(Arrays.asList(this.topics));
            shutdown();
        }

        @Override
        public void startRead(RecordSender recordSender) {
            int i=0;
            Record record = null;
            while (isRun) {
                ConsumerRecords<String, String> records = consumer.poll(this.pollIntervalTime);//100ms 拉取一次数据
                if (records.count() == 0) break;
                for (ConsumerRecord<String, String> item : records) {
                    record = recordSender.createRecord();
                    record.addColumn(new StringColumn(item.value()));
                    recordSender.sendToWriter(record);
                    i++;
                    if (i >= 100) {
                        consumer.commitAsync();//手动commit
                        i = 0;
                    }
                }
            }
        }


        public void  shutdown(){
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    isRun=false;
                    consumer.close();
                }
            });
        }

        @Override
        public void post() {
            super.post();
        }

        @Override
        public void destroy() {
        }


    }
}
