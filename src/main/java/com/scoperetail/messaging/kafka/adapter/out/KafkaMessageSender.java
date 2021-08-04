package com.scoperetail.messaging.kafka.adapter.out;

import org.springframework.kafka.core.KafkaTemplate;

public interface KafkaMessageSender {
  void send(String brokerId, String topicName, String payload);

  void registerKafkaTemplate(String brokerId, KafkaTemplate kafkaTemplate);
}
