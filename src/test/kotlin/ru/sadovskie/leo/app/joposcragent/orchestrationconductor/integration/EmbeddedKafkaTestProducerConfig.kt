package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.integration

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.utils.KafkaTestUtils

@TestConfiguration
class EmbeddedKafkaTestProducerConfig {

	@Bean
	@Primary
	fun consumerFactory(embeddedKafka: EmbeddedKafkaBroker): ConsumerFactory<String, String> =
		DefaultKafkaConsumerFactory(
			KafkaTestUtils.consumerProps("test-group", "true", embeddedKafka).apply {
				put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
				put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
				put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
			},
		)

	@Bean(name = ["kafkaListenerContainerFactory"])
	@Primary
	fun kafkaListenerContainerFactory(
		cf: ConsumerFactory<String, String>,
	): ConcurrentKafkaListenerContainerFactory<String, String> {
		val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
		factory.setConsumerFactory(cf)
		return factory
	}

	@Bean
	@Primary
	fun kafkaTemplate(embeddedKafka: EmbeddedKafkaBroker): KafkaTemplate<String, String> {
		val props = KafkaTestUtils.producerProps(embeddedKafka)
		props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
		props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
		return KafkaTemplate(DefaultKafkaProducerFactory(props))
	}
}
