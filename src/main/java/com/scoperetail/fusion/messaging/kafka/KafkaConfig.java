package com.scoperetail.fusion.messaging.kafka;

/*-
 * *****
 * fusion-messaging-kafka
 * -----
 * Copyright (C) 2018 - 2021 Scope Retail Systems Inc.
 * -----
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * =====
 */

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.CollectionUtils;
import com.scoperetail.fusion.config.Adapter;
import com.scoperetail.fusion.config.Broker;
import com.scoperetail.fusion.config.Broker.JmsProvider;
import com.scoperetail.fusion.config.FusionConfig;
import com.scoperetail.fusion.config.KafkaProducer;
import com.scoperetail.fusion.config.KafkaSecurityConfig;
import com.scoperetail.fusion.messaging.kafka.adapter.out.KafkaMessageSender;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
@AllArgsConstructor
public class KafkaConfig implements InitializingBean {
  private static final String CUSTOM_KAFKA_FACTORY = "_CUSTOM_KAFKA_FACTORY";
  private static final String CUSTOM_KAFKA_TEMPLATE = "_CUSTOM_KAFKA_TEMPLATE";

  private ApplicationContext applicationContext;

  private FusionConfig fusionConfig;

  private KafkaMessageSender kafkaMessageSender;

  private final Map<String, DefaultKafkaProducerFactory> connectionFactoryByBrokerIdMap =
      new HashMap<>(1);

  @Override
  public void afterPropertiesSet() throws Exception {
    final BeanDefinitionRegistry registry = getBeanDefinitionRegistry();
    registerKafkaBrokers(registry);
    registerKafkaTemplates(registry, getTargetBrokers());
  }

  private BeanDefinitionRegistry getBeanDefinitionRegistry() {
    return (BeanDefinitionRegistry) applicationContext.getAutowireCapableBeanFactory();
  }

  private void registerKafkaBrokers(final BeanDefinitionRegistry registry) {

    final List<Broker> kafkaBrokers = getKafkaBrokers();
    if (!CollectionUtils.isEmpty(kafkaBrokers)) {
      kafkaBrokers.forEach(broker -> registerKafkaBroker(broker, registry));
    }
  }

  private List<Broker> getKafkaBrokers() {
    return fusionConfig
        .getBrokers()
        .stream()
        .filter(broker -> broker.getJmsProvider().equals(JmsProvider.APACHEKAFKA))
        .collect(Collectors.toList());
  }

  private void registerKafkaBroker(final Broker broker, final BeanDefinitionRegistry registry) {

    final Map<String, Object> configs = getConfigs(broker);

    final BeanDefinitionBuilder factoryBeanDefinitionBuilder =
        BeanDefinitionBuilder.rootBeanDefinition(DefaultKafkaProducerFactory.class)
            .addConstructorArgValue(configs);

    final String factoryName = broker.getBrokerId() + CUSTOM_KAFKA_FACTORY;
    registry.registerBeanDefinition(factoryName, factoryBeanDefinitionBuilder.getBeanDefinition());

    final DefaultKafkaProducerFactory defaultKafkaProducerFactory =
        (DefaultKafkaProducerFactory) applicationContext.getBean(factoryName);

    connectionFactoryByBrokerIdMap.put(factoryName, defaultKafkaProducerFactory);
  }

  public Map<String, Object> getConfigs(final Broker broker) {
    final Map<String, Object> configProps = new HashMap<>();
    final KafkaProducer kafkaProducer = broker.getKafkaProducer();
    final KafkaSecurityConfig kafkaSecurityConfig = broker.getKafkaSecurityConfig();

    configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getHostUrl());
    configProps.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaProducer.getKeySerializerClass());
    configProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaProducer.getValueSerializerClass());
    configProps.put("retry.backoff.ms", kafkaProducer.getRetryBackoffMs());
    configProps.put("retries", kafkaProducer.getRetries());
    configProps.put(SaslConfigs.SASL_MECHANISM, kafkaSecurityConfig.getSaslMechanism());
    configProps.put("security.protocol", kafkaSecurityConfig.getSecurityProtocol());
    configProps.put(SaslConfigs.SASL_JAAS_CONFIG, kafkaSecurityConfig.getSaslJaasConfig());
    configProps.put("ssl.truststore.location", kafkaSecurityConfig.getSslTruststoreLocation());
    configProps.put("ssl.truststore.password", kafkaSecurityConfig.getSslTruststorePassword());
    configProps.put("listener.ack-mode", kafkaSecurityConfig.getListenerAckMode());
    return configProps;
  }

  private Set<String> getTargetBrokers() {
    final Set<String> uniqueBrokerIds = new HashSet<>();
    fusionConfig
        .getUsecases()
        .forEach(
            usecase ->
                fusionConfig
                    .getActiveConfig(usecase.getName())
                    .ifPresent(
                        config -> {
                          final List<Adapter> adapters =
                              config
                                  .getAdapters()
                                  .stream()
                                  .filter(
                                      c ->
                                          c.getAdapterType().equals(Adapter.AdapterType.OUTBOUND)
                                              && c.getTrasnportType()
                                                  .equals(Adapter.TransportType.KAFKA))
                                  .collect(Collectors.toList());
                          uniqueBrokerIds.addAll(
                              adapters
                                  .stream()
                                  .map(Adapter::getBrokerId)
                                  .collect(Collectors.toSet()));
                        }));
    return uniqueBrokerIds;
  }

  private void registerKafkaTemplates(
      final BeanDefinitionRegistry registry, final Set<String> targetBrokerIds) {
    targetBrokerIds.forEach(brokerId -> registerKafkaTemplate(registry, brokerId));
  }

  private void registerKafkaTemplate(final BeanDefinitionRegistry registry, final String brokerId) {
    final String factoryName = brokerId + CUSTOM_KAFKA_FACTORY;

    final DefaultKafkaProducerFactory connectionFactory =
        connectionFactoryByBrokerIdMap.get(factoryName);

    final BeanDefinitionBuilder templateBeanDefinitionBuilder =
        BeanDefinitionBuilder.rootBeanDefinition(KafkaTemplate.class)
            .addConstructorArgValue(connectionFactory);

    registry.registerBeanDefinition(
        brokerId + CUSTOM_KAFKA_TEMPLATE, templateBeanDefinitionBuilder.getBeanDefinition());

    final KafkaTemplate kafkaTemplate =
        (KafkaTemplate) applicationContext.getBean(brokerId + CUSTOM_KAFKA_TEMPLATE);

    kafkaMessageSender.registerKafkaTemplate(brokerId, kafkaTemplate);

    log.info("Registered KAFKA template for brokerId: {}", brokerId);
  }
}
