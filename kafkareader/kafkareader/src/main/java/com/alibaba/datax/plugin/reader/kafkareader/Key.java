package com.alibaba.datax.plugin.reader.kafkareader;

/**
 * Created by chuanyuan_qu@163.com on 2017/7/17.
 */
public class Key {

    //bootstrapServers列表
    public static  final  String BOOTSTRAP_SERVERS="bootstrapServers";
    //主题名称
    public static final String TOPICS="topics";
    //键序列化  序列化处理类，消息在网络上传输就需要序列化，它有String、数组等许多种实现。
    public static  final  String KEY_SERIALIZER="keySerializer";
    //值序列化  序列化处理类，消息在网络上传输就需要序列化，它有String、数组等许多种实现。
    public static  final  String VALUE_SERIALIZER="valueSerializer";
    //消费组id
    public static final String GROUP_ID="groupId";
    //消费的时间间隔
    public static final String POLL_INTERVAL_TIME="pollIntervalTime";
    //自动/手动提交 false 手动 true 自动
    public static final String AUTO_COMMIT="autoCommit";
    //提交间隔条数   只有当AUTO_COMMIT设置为false时候才会生效
    public static final String COMMIT_INTERVAL="commitInterval";
}
