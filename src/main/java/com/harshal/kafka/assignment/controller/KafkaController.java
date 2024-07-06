package com.harshal.kafka.assignment.controller;

import com.harshal.kafka.assignment.consumer.KafkaMessageConsumer;
import com.harshal.kafka.assignment.model.Employee;
import com.harshal.kafka.assignment.producer.KafkaProducer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller for Kafka operations.
 */
@RestController
@RequestMapping("/kafka")
public class KafkaController {

  private final KafkaProducer kafkaProducer;

  private final KafkaMessageConsumer kafkaMessageConsumer;

  /**
   * Constructs a KafkaController instance.
   *
   * @param kafkaProducer        KafkaProducer instance for sending messages.
   * @param kafkaMessageConsumer KafkaMessageConsumer instance for consuming messages.
   */
  public KafkaController(KafkaProducer kafkaProducer, KafkaMessageConsumer kafkaMessageConsumer) {
    this.kafkaProducer = kafkaProducer;
    this.kafkaMessageConsumer = kafkaMessageConsumer;
  }

  /**
   * Sends a String message to Kafka.
   *
   * @param message The message to send.
   * @return A response indicating the success of the operation.
   */
  @PostMapping("/sendString")
  public String sendStringMessage(@RequestParam("message") String message) {
    kafkaProducer.sendMessage(message);
    return "Message sent: " + message;
  }

  /**
   * Sends a JSON message (Employee object) to Kafka.
   *
   * @param employee The Employee object to send.
   * @return A response indicating the success of the operation.
   */
  @PostMapping("/sendJson")
  public String sendJsonMessage(@RequestBody Employee employee) {
    kafkaProducer.sendJsonMessage(employee);
    return "JSON Message sent to Kafka topic";
  }

  /**
   * Sets the consumer offsets for consuming String messages from Kafka.
   *
   * @param startOffset The starting offset to consume messages from.
   * @param endOffset   The ending offset (exclusive) to stop consuming messages.
   * @return ResponseEntity containing a list of consumed messages or an error message.
   */
  @PostMapping("/setOffsets")
  public ResponseEntity<?> setOffsets(@RequestParam long startOffset,
      @RequestParam long endOffset) {
    try {
      return ResponseEntity.ok("Consumed messages between offset range are: " +
          kafkaMessageConsumer.consumeMessages(startOffset, endOffset));
    } catch (Exception e) {
      return ResponseEntity
          .status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body(e.getMessage());
    }
  }

  /**
   * Sets the consumer offsets for consuming JSON messages (Employee) from Kafka.
   *
   * @param startOffset The starting offset to consume messages from.
   * @param endOffset   The ending offset (inclusive) to stop consuming messages.
   * @return ResponseEntity containing a list of consumed JSON messages or an error message.
   */
  @PostMapping("/setJsonOffsets")
  public ResponseEntity<?> setJsonOffsets(@RequestParam long startOffset,
      @RequestParam long endOffset) {
    try {
      return ResponseEntity.ok("Consumed JSON messages between offset range are: " +
          kafkaMessageConsumer.consumeJsonMessages(startOffset, endOffset));
    } catch (Exception e) {
      return ResponseEntity
          .status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body(e.getMessage());
    }
  }

}
