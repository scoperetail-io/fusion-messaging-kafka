package com.scoperetail.fusion.messaging.kafka.adapter.out.impl;

/*-
 * *****
 * fusion-messaging-kafka
 * -----
 * Copyright (C) 2018 - 2021 Scope Retail Systems Inc.
 * -----
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =====
 */

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
