package ru.sadovskie.leo.app.joposcragent.orchestrationconductor.kafka

object OrchestrationKafkaTopics {
	const val COLLECTION_BATCH = "async-job.collection-batch"
	const val COLLECTION_QUERY = "async-job.collection-query"
	const val JOB_POSTING_EVALUATE = "async-job.job-posting-evaluate"
	const val JOB_POSTING_CREATE = "async-job.job-posting-create"
}

object OrchestrationMessageTypes {
	const val COLLECTION_BATCH_BEGIN = "async-job.collection-batch-begin"
	const val COLLECTION_QUERY_BEGIN = "async-job.collection-query-begin"
	const val COLLECTION_QUERY_RESULT = "async-job.collection-query-result"
	const val COLLECTION_BATCH_RESULT = "async-job.collection-batch-result"
	const val JOB_POSTING_CREATE_RESULT = "async-job.job-posting-create-result"
	const val JOB_POSTING_EVALUATE_BEGIN = "async-job.job-posting-evaluate-begin"
}
