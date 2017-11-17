package com.alibaba.datax.plugin.writer.kafkawriter;


public class Key {
    //kafka节点列表
    public static  final  String BOOTSTRAP_SERVERS="bootstrapServers";
    //发送数据是否需要服务端的反馈,有三个值0,1,-1，(默认0)分别代表3种状态
    public static  final  String ACKS="acks";
    //是否压缩 默认0表示不压缩，1表示用gzip压缩，2表示用snappy压缩。 默认none
    public static  final  String COMPRESSION="compression";
    //键序列化  序列化处理类，消息在网络上传输就需要序列化，它有String、数组等许多种实现。
    public static  final  String KEY_SERIALIZER="keySerializer";
    //值序列化  序列化处理类，消息在网络上传输就需要序列化，它有String、数组等许多种实现。
    public static  final  String VALUE_SERIALIZER="valueSerializer";
    //客户端名称
    public static final String CLIENT_ID_CONFIG="clientId";
    //主题名称
    public static final String TOPICS="topics";
    //socket的发送缓冲区
    public static final String  SEND_BUFFER_BYTES= "sendBufferBytes";
    //一般我们会选择异步。同步还是异步发送消息，默认“sync”表同步，"async"表异步。异步可以提高发送吞吐量,也意味着消息将会在本地buffer中,并适时批量发送，但是也可能导致丢失未发送过去的消息
    public static final String IS_SYNC="isAsync";
    //同步的参数
    public static final String ATTRIBUTE_NAME_STRING="attributeNames";
    //单条消息发送失败的话 是否重试 默认false
    public static final String IS_RETRY="isRetry";
}
