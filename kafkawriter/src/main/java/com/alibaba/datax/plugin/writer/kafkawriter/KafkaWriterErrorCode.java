package com.alibaba.datax.plugin.writer.kafkawriter;

import com.alibaba.datax.common.spi.ErrorCode;


public enum  KafkaWriterErrorCode implements ErrorCode {
    REQUIRED_VALUE("KafkaWriter-00", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("KafkaWriter-01", "您填写的参数值不合法."),
    MIXED_INDEX_VALUE("KafkaWriter-02", "您的列信息配置同时包含了index,value."),
    NO_INDEX_VALUE("KafkaWriter-03","您明确的配置列信息,但未填写相应的index,value."),

    TOPIC_NOT_EXISTS("kafkaWriter-04", "您配置的topic不存在."),
    FAIL_ZK_LOGIN("KafkaWriter-05", "连接失败,无法与服务器建立连接.");


    private final String code;
    private final String description;

    private KafkaWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }
}
