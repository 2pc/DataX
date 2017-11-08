package com.alibaba.datax.plugin.writer.kafkawriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by quchuanyuan on 2017/7/17.
 */
public class KafkaWriter {

    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfiguration = null;

        @Override
        public void init() {
            this.originalConfiguration = this.getPluginJobConf();
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
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> writerSplitConfiguration = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                writerSplitConfiguration.add(this.originalConfiguration);
            }
            return writerSplitConfiguration;
        }
    }


    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private Configuration writerSliceConfig;

        private static String bootstrapServers;

        private static String clientIdConfig;

        private static String acks;

        private static String compressionType;

        private static String keySerializer;

        private static String valueSerializer;

        private static Integer sendBufferBytes;

        private static Boolean isAsync = false;

        private static String attributeNameStr;

        private static String[] attributeNames;

        private static String topics;

        private static Boolean isRetry;


        private static Producer<String, String> producer = null;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.bootstrapServers = this.writerSliceConfig.getString(Key.BOOTSTRAP_SERVERS, null);
            this.clientIdConfig = this.writerSliceConfig.getString(Key.CLIENT_ID_CONFIG, null);
            this.acks = this.writerSliceConfig.getString(Key.ACKS, null);
            this.compressionType = this.writerSliceConfig.getString(Key.COMPRESSION, null);
            this.keySerializer = this.writerSliceConfig.getString(Key.KEY_SERIALIZER, null);
            this.valueSerializer = this.writerSliceConfig.getString(Key.VALUE_SERIALIZER, null);
            this.sendBufferBytes = this.writerSliceConfig.getInt(Key.SEND_BUFFER_BYTES, 0);
            this.topics = this.writerSliceConfig.getString(Key.TOPICS, null);
            this.isAsync = this.writerSliceConfig.getBool(Key.IS_SYNC, false);
            this.attributeNameStr = this.writerSliceConfig.getString(Key.ATTRIBUTE_NAME_STRING, null);

            this.isRetry = this.writerSliceConfig.getBool(Key.IS_RETRY, false);

            if (StringUtils.isEmpty(bootstrapServers)) {
                throw DataXException
                        .asDataXException(
                                KafkaWriterErrorCode.REQUIRED_VALUE,
                                String.format("请检查名称: [%s].",
                                        Key.BOOTSTRAP_SERVERS));
            }
            if (StringUtils.isNotEmpty(attributeNameStr)) {
                attributeNames = attributeNameStr.split(",");
            }
        }

        @Override
        public void prepare() {
            Properties props = new Properties();
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, clientIdConfig);
            if (StringUtils.isNotBlank(acks)) {
                props.put(ProducerConfig.ACKS_CONFIG, acks);
            }
            if (StringUtils.isNotBlank(compressionType)) {
                props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
            }
            if (StringUtils.isNotBlank(keySerializer)) {
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
            }
            if (StringUtils.isNotBlank(valueSerializer)) {
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
            }
            if (sendBufferBytes != 0) {
                props.put(ProducerConfig.SEND_BUFFER_CONFIG, sendBufferBytes);
            }

            producer = new KafkaProducer<String, String>(props);
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            List<Record> writerBuffer = new ArrayList<Record>(this.sendBufferBytes);
            Record record = null;
            while ((record = lineReceiver.getFromReader()) != null) {
                writerBuffer.add(record);
                if (writerBuffer.size() >= this.sendBufferBytes) {
                    sendKafka(writerBuffer);
                    writerBuffer.clear();
                }
            }
            if (!writerBuffer.isEmpty()) {
                sendKafka(writerBuffer);
                writerBuffer.clear();
            }

            producer.flush();
            producer.close();
        }

        private void sendKafka(List<Record> writerBuffer) {
            String key = clientIdConfig;
            String msg="";
            for (int w = 0, wlen = writerBuffer.size(); w < wlen; w++) {
                if(attributeNames!=null){
                    msg=transformMsgRecordWithKey(writerBuffer.get(w));
                }else{
                    msg=transformMsgRecord(writerBuffer.get(w));
                }

                try {
                    if (isAsync) {
                        //异步
                        producer.send(new ProducerRecord<String, String>(this.topics, key, msg),
                                new MsgProducerCallback(System.currentTimeMillis(), key, msg));
                    } else {
                        //同步
                        producer.send(new ProducerRecord<String, String>(this.topics, key, msg)).get();
                    }
                } catch (InterruptedException e) {
                    LOG.error(e.getMessage());
                } catch (ExecutionException e) {
                    LOG.error(e.getMessage());
                }
            }
        }

        private static String  transformMsgRecord(Record record){
            StringBuffer msgBuffer=new StringBuffer();
            int fieldNum = record.getColumnNumber();
            if (null == record && fieldNum <= 0) {
                return null;
            }
            for (int i = 0; i < fieldNum; i++) {
                msgBuffer.append(record.getColumn(i).asString()).append(",");
            }
            if(StringUtils.isNotEmpty(msgBuffer)){
                msgBuffer.substring(0,msgBuffer.length()-1);
            }
            return msgBuffer.toString();
        }
        private static String  transformMsgRecordWithKey(Record record){
            Map<String, String> attributeValueMap = null;
            attributeValueMap = new HashMap<String, String>();

            int fieldNum = record.getColumnNumber();
            if (null == record && fieldNum <= 0) {
                return null;
            }
            for (int i = 0; i < fieldNum; i++) {
                attributeValueMap.put(attributeNames[i].toLowerCase(), record.getColumn(i).asString());
            }
            return attributeValueMap.toString();
        }

        /**
         * properties.getProperty
         * 消息发送后的回调函数
         */
        class MsgProducerCallback implements Callback {

            private final String key;
            private final String msg;

            public MsgProducerCallback(long startTime, String key, String msg) {
                this.key = key;
                this.msg = msg;
            }

            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (isRetry) {
                    if (e != null) {
                        producer.send(new ProducerRecord<String, String>(topics, key, msg));
                    }
                }
            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }
}
