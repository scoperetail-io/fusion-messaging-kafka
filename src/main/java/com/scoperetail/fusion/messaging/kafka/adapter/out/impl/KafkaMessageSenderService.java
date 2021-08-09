package com.scoperetail.fusion.messaging.kafka.adapter.out.impl;

import java.util.HashMap;
import java.util.Map;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.scoperetail.fusion.messaging.kafka.adapter.out.KafkaMessageSender;

@Service
public class KafkaMessageSenderService implements KafkaMessageSender {
  private final Map<String, KafkaTemplate> kafkaTemplateByBrokerIdMap = new HashMap<>();

  @Override
  public void send(final String brokerId, final String topicName, final String payload) {
    final KafkaTemplate kafkaTemplate = kafkaTemplateByBrokerIdMap.get(brokerId);
    kafkaTemplate.send(topicName, payload);
  }

  @Override
  public void registerKafkaTemplate(final String brokerId, final KafkaTemplate kafkaTemplate) {
    kafkaTemplateByBrokerIdMap.put(brokerId, kafkaTemplate);
  }
}
