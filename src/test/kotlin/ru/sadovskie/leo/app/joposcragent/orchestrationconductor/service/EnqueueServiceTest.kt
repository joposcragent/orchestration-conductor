package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.service

import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.kafka.OrchestrationKafkaPublisher
import ru.sadovskie.leo.app.joposcragent.orchestrationconductor.openapi.model.SearchQueryItem
import java.util.UUID

class EnqueueServiceTest {

	private val publisher = mockk<OrchestrationKafkaPublisher>(relaxUnitFun = true)
	private val service = EnqueueService(publisher)

	@Test
	fun `enqueue collection batch publishes begin`() {
		val slot = slot<UUID>()
		every { publisher.publishCollectionBatchBegin(capture(slot)) } just Runs

		val out = service.enqueueCollectionBatch()

		assertEquals(slot.captured, out)
		verify(exactly = 1) { publisher.publishCollectionBatchBegin(out) }
	}

	@Test
	fun `enqueue collection query passes lazy flag`() {
		val id = UUID.randomUUID()
		val body = SearchQueryItem("q", id, true)
		every { publisher.publishCollectionQueryBegin(any(), any(), any(), any(), null) } returns Unit

		service.enqueueCollectionQuery(body)

		verify(exactly = 1) { publisher.publishCollectionQueryBegin(any(), "q", id, true, null) }
	}
}
