package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.kafka

import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.kotlinModule
import java.util.UUID
import java.util.concurrent.CompletableFuture

class OrchestrationKafkaPublisherTest {

	private val jsonMapper = JsonMapper.builder().addModule(kotlinModule()).build()

	@Test
	fun `publishEnvelope sets kafka headers and json envelope`() {
		val template = mockk<KafkaTemplate<String, String>>()
		val slot = slot<ProducerRecord<String, String>>()
		every { template.send(capture(slot)) } returns CompletableFuture.completedFuture(mockk<SendResult<String, String>>(relaxed = true))
		val publisher = OrchestrationKafkaPublisher(template, jsonMapper)

		val job = UUID.randomUUID()
		publisher.publishCollectionBatchBegin(job)

		val record = slot.captured
		assertEquals(OrchestrationKafkaTopics.COLLECTION_BATCH, record.topic())
		assertEquals(job.toString(), record.key())
		val text = record.value()
		assertTrue(text.contains("async-job.collection-batch-begin"))
		assertTrue(text.contains("\"jobUuid\":\"$job\""))
	}
}
