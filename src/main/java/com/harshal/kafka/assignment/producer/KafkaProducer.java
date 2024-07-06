package com.harshal.kafka.assignment.producer;

import com.harshal.kafka.assignment.model.Employee;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Kafka message producer component that sends messages to configured Kafka topics.
 */
@Component
@Slf4j
public class KafkaProducer {

  @Value("${kafka.topic.name}")
  private String topic;

  @Value("${kafka.topic.json-name}")
  private String jsonTopic;

  private final KafkaTemplate<String, String> kafkaTemplate;

  private final KafkaTemplate<String, Employee> kafkaJsonTemplate;

  /**
   * Constructs a KafkaProducer instance.
   *
   * @param kafkaTemplate     KafkaTemplate for sending String messages.
   * @param kafkaJsonTemplate KafkaTemplate for sending JSON messages (Employee).
   */
  public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate,
      KafkaTemplate<String, Employee> kafkaJsonTemplate) {
    this.kafkaTemplate = kafkaTemplate;
    this.kafkaJsonTemplate = kafkaJsonTemplate;
  }

  /**
   * Sends a String message to the configured Kafka topic.
   *
   * @param message The message to send.
   */
  public void sendMessage(String message) {
    kafkaTemplate.send(topic, message);
    log.info("String message sent to topic {}: {}", topic, message);
  }

  /**
   * Sends a JSON message (Employee) to the configured Kafka topic.
   *
   * @param employee The Employee object to send as JSON.
   */
  public void sendJsonMessage(Employee employee) {
    kafkaJsonTemplate.send(jsonTopic, employee);
    log.info("JSON message sent to topic {}: {}", jsonTopic, employee.toString());
  }

}