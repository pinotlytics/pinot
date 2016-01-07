/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.realtime.impl.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.stream.KafkaStreamMetadata;
import com.linkedin.pinot.common.utils.CommonConstants.Helix;
import com.linkedin.pinot.core.realtime.StreamProviderConfig;

import kafka.consumer.ConsumerConfig;

public class KafkaHighLevelStreamProviderConfig implements StreamProviderConfig {
  private static final Map<String, String> defaultProps;

  public static final int FIVE_MILLION = 5000000;
  private final static long ONE_MINUTE_IN_MILLSEC = 1000 * 60;
  public static final long ONE_HOUR = ONE_MINUTE_IN_MILLSEC * 60;

  static {
    defaultProps = new HashMap<String, String>();
    /*defaultProps.put("zookeeper.connect", zookeeper);
    defaultProps.put("group.id", groupId);*/
    defaultProps.put("zookeeper.session.timeout.ms", "30000");
    defaultProps.put("zookeeper.sync.time.ms", "200");
    defaultProps.put("auto.commit.enable", "false");
    defaultProps.put("auto.offset.reset", "largest");
  }

  private String kafkaTopicName;
  private String zkString;
  private String groupId;
  private KafkaMessageDecoder decoder;
  private String decodeKlass;
  private Schema indexingSchema;
  private Map<String, String> decoderProps;
  private Map<String, String> kafkaConsumerProps;
  private Map<String, String> consumerProps;
  private long segmentTimeInMillis = ONE_HOUR;
  private int realtimeRecordsThreshold = FIVE_MILLION;

  /*
   * kafka.hlc.zk.connect.string : comma separated list of hosts
   * kafka.hlc.broker.port : broker port
   * kafka.hlc.group.id : group id
   * kafka.decoder.class.name : the absolute path of the decoder class name
   * kafka.decoder.props1 : every property that is prefixed with kafka.decoder.
   * */

  @Override
  public void init(Map<String, String> properties, Schema schema) {
    decoderProps = new HashMap<String, String>();
    kafkaConsumerProps = new HashMap<>();
    consumerProps = new HashMap<String, String>(defaultProps);
    this.indexingSchema = schema;
    if (properties.containsKey(Helix.DataSource.Realtime.Kafka.HighLevelConsumer.GROUP_ID)) {
      this.groupId = properties.get(Helix.DataSource.Realtime.Kafka.HighLevelConsumer.GROUP_ID);
    }

    if (properties.containsKey(Helix.DataSource.Realtime.Kafka.HighLevelConsumer.ZK_CONNECTION_STRING)) {
      this.zkString = properties.get(Helix.DataSource.Realtime.Kafka.HighLevelConsumer.ZK_CONNECTION_STRING);
    }

    if (properties.containsKey(Helix.DataSource.Realtime.Kafka.TOPIC_NAME)) {
      this.kafkaTopicName = properties.get(Helix.DataSource.Realtime.Kafka.TOPIC_NAME);
    }

    if (properties.containsKey(Helix.DataSource.Realtime.Kafka.DECODER_CLASS)) {
      this.decodeKlass = properties.get(Helix.DataSource.Realtime.Kafka.DECODER_CLASS);
    }

    if (groupId == null || zkString == null || kafkaTopicName == null || this.decodeKlass == null) {
      throw new RuntimeException("Cannot initialize KafkaHighLevelStreamProviderConfig as: " + "groupId = " + groupId
          + ", zkString = " + zkString + ", kafkaTopicName = " + kafkaTopicName + ", decodeKlass = " + decodeKlass);
    }

    if (properties.containsKey(Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE)) {
      realtimeRecordsThreshold =
          Integer.parseInt(properties.get(Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE));
    }

    if (properties.containsKey(Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_TIME)) {
      segmentTimeInMillis =
          Long.parseLong(properties.get(Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_TIME));
    }

    if (properties.containsKey(Helix.DataSource.Realtime.Kafka.HighLevelConsumer.AUTO_OFFSET_RESET)) {
      String autoOffsetRest = properties.get(Helix.DataSource.Realtime.Kafka.HighLevelConsumer.AUTO_OFFSET_RESET);
      consumerProps.put("auto.offset.reset", autoOffsetRest);
    }

    for (String key : properties.keySet()) {
      if (key.startsWith(Helix.DataSource.Realtime.Kafka.DECODER_PROPS_PREFIX)) {
        decoderProps.put(Helix.DataSource.Realtime.Kafka.getDecoderPropertyKey(key), properties.get(key));
      }

      if (key.startsWith(Helix.DataSource.Realtime.Kafka.KAFKA_CONSUMER_PROPS_PREFIX)) {
        kafkaConsumerProps.put(Helix.DataSource.Realtime.Kafka.getConsumerPropertyKey(key), properties.get(key));
      }
    }
  }

  @Override
  public Schema getSchema() {
    return indexingSchema;
  }

  public String getTopicName() {
    return this.kafkaTopicName;
  }

  public Map<String, Integer> getTopicMap(int numThreads) {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(kafkaTopicName, new Integer(numThreads));
    return topicCountMap;
  }

  public ConsumerConfig getKafkaConsumerConfig() {
    Properties props = new Properties();
    for (String key : defaultProps.keySet()) {
      props.put(key, defaultProps.get(key));
    }

    for (String key : kafkaConsumerProps.keySet()) {
      props.put(key, kafkaConsumerProps.get(key));
    }

    props.put("group.id", groupId);
    props.put("zookeeper.connect", zkString);
    return new ConsumerConfig(props);
  }

  public KafkaMessageDecoder getDecoder() throws Exception {
    KafkaMessageDecoder ret = (KafkaMessageDecoder) Class.forName(decodeKlass).newInstance();
    ret.init(decoderProps, indexingSchema, kafkaTopicName);
    return ret;
  }

  @Override
  public String getStreamProviderClass() {
    return null;
  }

  @Override
  public void init(AbstractTableConfig tableConfig, InstanceZKMetadata instanceMetadata, Schema schema) {
    this.indexingSchema = schema;
    this.groupId = instanceMetadata.getGroupId(tableConfig.getTableName());
    KafkaStreamMetadata kafkaMetadata = new KafkaStreamMetadata(tableConfig.getIndexingConfig().getStreamConfigs());
    this.kafkaTopicName = kafkaMetadata.getKafkaTopicName();
    this.decodeKlass = kafkaMetadata.getDecoderClass();
    this.decoderProps = kafkaMetadata.getDecoderProperties();
    this.kafkaConsumerProps = kafkaMetadata.getKafkaConsumerProperties();
    this.zkString = kafkaMetadata.getZkBrokerUrl();
    if (tableConfig.getIndexingConfig().getStreamConfigs().containsKey(Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE)) {
      realtimeRecordsThreshold =
          Integer.parseInt(tableConfig.getIndexingConfig().getStreamConfigs().get(Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE));
    }

    if (tableConfig.getIndexingConfig().getStreamConfigs().containsKey(Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_TIME)) {
      segmentTimeInMillis =
          Long.parseLong(tableConfig.getIndexingConfig().getStreamConfigs().get(Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_TIME));
    }

  }

  @Override
  public int getSizeThresholdToFlushSegment() {
    return realtimeRecordsThreshold;
  }

  @Override
  public long getTimeThresholdToFlushSegment() {
    return segmentTimeInMillis;
  }
}
