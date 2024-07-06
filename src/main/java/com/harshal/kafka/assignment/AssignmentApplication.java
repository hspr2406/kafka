package com.harshal.kafka.assignment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The main entry point for the Kafka Assignment application.
 * This Spring Boot application initializes and runs the Kafka producer and consumer.
 * The application.yml file can be used to configure various properties such as Kafka bootstrap servers,
 * serializers, and consumer group ID.
 */
@SpringBootApplication
public class AssignmentApplication {

  /**
   * Main method to start the Kafka Assignment application.
   */
  public static void main(String[] args) {
    SpringApplication.run(AssignmentApplication.class, args);
  }

}

