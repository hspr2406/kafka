package com.harshal.kafka.assignment.config;

import com.harshal.kafka.assignment.model.Employee;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for setting up Kafka consumers.
 */
@Configuration
public class KafkaConsumerConfig {

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServerUrl;

  @Value("${spring.kafka.consumer.group-id:}")
  private String groupId;

  /**
   * Configures a Kafka consumer factory for consuming String messages.
   *
   * @return ConsumerFactory for String messages.
   */
  @Bean
  public ConsumerFactory<String, String> consumerFactory() {
    Map<String, Object> configProps = new HashMap<>();
    configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerUrl);
    configProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return new DefaultKafkaConsumerFactory<>(configProps);
  }

  /**
   * Configures a Kafka listener container factory for String messages.
   *
   * @return ConcurrentKafkaListenerContainerFactory for String messages.
   */
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());
    return factory;
  }

  /**
   * Configures a Kafka consumer factory for consuming JSON messages and deserializing them into
   * Employee objects.
   *
   * @return ConsumerFactory for JSON messages mapped to Employee objects.
   */
  @Bean
  public ConsumerFactory<String, Employee> consumerJsonFactory() {
    Map<String, Object> configProps = new HashMap<>();
    configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerUrl);
    configProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
    configProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Employee.class);
    configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.harshal.kafka.assignment.model");

    return new DefaultKafkaConsumerFactory<>(configProps, new StringDeserializer(),
        new JsonDeserializer<>(Employee.class));
  }

  /**
   * Configures a Kafka listener container factory for JSON messages mapped to Employee objects.
   *
   * @return ConcurrentKafkaListenerContainerFactory for JSON messages mapped to Employee objects.
   */
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, Employee> kafkaListenerJsonFactory() {
    ConcurrentKafkaListenerContainerFactory<String, Employee> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerJsonFactory());
    factory.setBatchListener(true); // Enable batch listening if required
    return factory;
  }

}