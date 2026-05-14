package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.integration

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.ResponseEntity
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.client.SettingsSearchQueryFeignClient
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.client.model.SearchQueriesItem
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.kafka.OrchestrationKafkaTopics
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.kafka.OrchestrationMessageTypes
import org.springframework.test.context.bean.override.mockito.MockitoBean
import java.time.Duration
import java.time.OffsetDateTime
import java.util.UUID

@SpringBootTest
@Import(EmbeddedKafkaTestProducerConfig::class)
@EmbeddedKafka(
	partitions = 1,
	topics = [
		OrchestrationKafkaTopics.COLLECTION_BATCH,
		OrchestrationKafkaTopics.COLLECTION_QUERY,
	],
)
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class CollectionBatchKafkaIntegrationTest {

	companion object {
		@JvmStatic
		@DynamicPropertySource
		fun kafkaProps(r: DynamicPropertyRegistry) {
			r.add("app.kafka.batch-consumer-group") { "test-conductor-batch-${UUID.randomUUID()}" }
			r.add("app.kafka.create-result-consumer-group") { "test-conductor-create-${UUID.randomUUID()}" }
		}
	}

	@Autowired
	private lateinit var kafkaTemplate: KafkaTemplate<String, String>

	@Autowired
	private lateinit var embeddedKafka: EmbeddedKafkaBroker

	@MockitoBean
	private lateinit var searchQueryClient: SettingsSearchQueryFeignClient

	private val now: OffsetDateTime = OffsetDateTime.parse("2026-01-01T12:00:00Z")

	@BeforeEach
	fun stubQueries() {
		val q = UUID.randomUUID()
		val item = SearchQueriesItem(q, "n", "my-query", 0.5, 0.5, true, false, now)
		whenever(searchQueryClient.listSearchQueries(true)).thenReturn(ResponseEntity.ok(listOf(item)))
	}

	@Test
	fun `consumes batch begin and publishes collection query`() {
		val parent = UUID.randomUUID()
		val json = """
			{"headers":{"key":"$parent","createdAt":"2026-01-01T12:00:00Z","type":"${OrchestrationMessageTypes.COLLECTION_BATCH_BEGIN}","schemaVersion":"1.0"},"payload":{"jobUuid":"$parent"}}
		""".trimIndent()
		kafkaTemplate.send(OrchestrationKafkaTopics.COLLECTION_BATCH, parent.toString(), json).get()

		val consumerProps = KafkaTestUtils.consumerProps("verify-${UUID.randomUUID()}", "true", embeddedKafka)
		consumerProps[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
		consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
		consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
		val consumer = KafkaConsumer<String, String>(consumerProps, StringDeserializer(), StringDeserializer())
		consumer.subscribe(listOf(OrchestrationKafkaTopics.COLLECTION_QUERY))
		val records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(15))
		assertEquals(1, records.count())
		val record = records.iterator().next()
		consumer.close()
		assertEquals(OrchestrationKafkaTopics.COLLECTION_QUERY, record.topic())
		assertTrue(record.value().contains(OrchestrationMessageTypes.COLLECTION_QUERY_BEGIN))
		assertTrue(record.value().contains("my-query"))
		assertTrue(record.value().contains("\"parentJobUuid\":\"$parent\""))
	}
}
