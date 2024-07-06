package com.harshal.kafka.assignment.consumer;

import com.harshal.kafka.assignment.model.Employee;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Kafka message consumer component that handles consuming messages from Kafka topics.
 */
@Component
@Slf4j
public class KafkaMessageConsumer {

  @Autowired
  private ConsumerFactory<String, String> consumerFactory;

  @Autowired
  private ConsumerFactory<String, Employee> consumerJsonFactory;

  @Value("${kafka.topic.name}")
  private String strTopic;

  @Value("${kafka.topic.json-name}")
  private String userJsonTopic;

  /**
   * Consumes String messages from the configured Kafka topic between the specified offsets.
   *
   * @param startOffset The starting offset to consume messages from.
   * @param endOffset   The ending offset (exclusive) to stop consuming messages.
   * @return A list of consumed String messages.
   */
  public List<String> consumeMessages(long startOffset, long endOffset) {
    List<String> messages = new ArrayList<>();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
        consumerFactory.getConfigurationProperties())) {
      List<TopicPartition> topicPartitions = new ArrayList<>();
      for (PartitionInfo info : consumer.partitionsFor(strTopic)) {
        TopicPartition topicPartition = new TopicPartition(strTopic, info.partition());
        topicPartitions.add(topicPartition);
      }

      consumer.assign(topicPartitions);

      for (TopicPartition topicPartition : topicPartitions) {
        consumer.seek(topicPartition, startOffset);
      }

      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
      for (ConsumerRecord<String, String> record : records) {
        if (record.offset() < endOffset) {
          log.info("Consumed String message: {}, offset: {}", record.value(), record.offset());
          messages.add(record.value());
        }
      }
      consumer.commitAsync();
    } catch (Exception e) {
      log.error("Error while consuming String messages: {}", e.getMessage());
      throw e;
    }
    return messages;
  }

  /**
   * Consumes JSON messages (Employee objects) from the configured Kafka topic between the specified
   * offsets.
   *
   * @param startOffset The starting offset to consume messages from.
   * @param endOffset   The ending offset (inclusive) to stop consuming messages.
   * @return A list of consumed Employee JSON messages.
   */
  public List<Employee> consumeJsonMessages(long startOffset, long endOffset) {
    List<Employee> jsonMessages = new ArrayList<>();
    try (KafkaConsumer<String, Employee> consumer = new KafkaConsumer<>(
        consumerJsonFactory.getConfigurationProperties())) {
      List<TopicPartition> topicPartitions = new ArrayList<>();
      for (PartitionInfo info : consumer.partitionsFor(userJsonTopic)) {
        TopicPartition topicPartition = new TopicPartition(userJsonTopic, info.partition());
        topicPartitions.add(topicPartition);
      }

      consumer.assign(topicPartitions);

      for (TopicPartition topicPartition : topicPartitions) {
        consumer.seek(topicPartition, startOffset);
      }

      ConsumerRecords<String, Employee> records = consumer.poll(Duration.ofSeconds(5));
      for (ConsumerRecord<String, Employee> record : records) {
        if (record.offset() > endOffset) {
          break;
        }
        log.info("Consumed Json message: {}, offset: {}", record.value(), record.offset());
        jsonMessages.add(record.value());
      }

      consumer.commitAsync();
    } catch (Exception e) {
      log.error("Error while consuming JSON messages: {}", e.getMessage());
      throw e;
    }
    return jsonMessages;
  }

}
