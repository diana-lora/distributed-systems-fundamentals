package com.example.dsf.config

import com.example.dsf.commons.OrderCreatedEvent
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.context.event.EventListener
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer
import org.springframework.util.backoff.FixedBackOff
import tools.jackson.databind.DeserializationFeature as KafkaDeserializationFeature
import tools.jackson.databind.json.JsonMapper as KafkaJsonMapper
import tools.jackson.module.kotlin.kotlinModule as kafkaKotlinModule

@EnableKafka
@Configuration
class KafkaConfig(
    @Value($$"${spring.kafka.bootstrap-servers}")
    private val bootstrapServers: String
) {

    private val logger = KotlinLogging.logger {}

    @Bean
    @Primary
    fun customObjectMapper(): ObjectMapper {
        return ObjectMapper().apply {
            registerModule(kotlinModule())
            configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        }
    }

    @Bean
    fun customKafkaJsonMapper(): KafkaJsonMapper {
        return KafkaJsonMapper.builder()
            .addModule(kafkaKotlinModule())
            .disable(KafkaDeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build()
    }

    @Bean
    fun kafkaAdmin(): KafkaAdmin {
        return KafkaAdmin(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers))
    }

    @Bean
    fun customProducerFactory(): ProducerFactory<Any, Any> {
        val configProps = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java.name
        )
        return DefaultKafkaProducerFactory(configProps)
    }

    @Bean
    fun customKafkaTemplate(producerFactory: ProducerFactory<Any, Any>) = KafkaTemplate(producerFactory)

    @Bean
    fun errorHandler(template: KafkaTemplate<Any, Any>): DefaultErrorHandler {
        // 1. If an error occurs, send the poison pill message to the Dead Letter Topic (topic-name.DLT) [source]
        val recoverer = DeadLetterPublishingRecoverer(template) { cr, _ ->
            TopicPartition(ORDER_EVENTS_DLT_TOPIC, cr.partition())
        }

        // 2. Retry policy: Retry 3 times with a 2-second interval before giving up [source]
        val backOff = FixedBackOff(2000L, 3L)

        val errorHandler = DefaultErrorHandler(recoverer, backOff)

        // 3. Do not retry if the error is purely due to fatal Deserialization issues [source]
        errorHandler.addNotRetryableExceptions(org.apache.kafka.common.errors.SerializationException::class.java)
        errorHandler.addNotRetryableExceptions(org.springframework.kafka.support.serializer.DeserializationException::class.java)

        return errorHandler
    }

    @Bean
    fun customConsumerFactory(kafkaJsonMapper: KafkaJsonMapper): ConsumerFactory<String, OrderCreatedEvent> {
        val configProps = mutableMapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java
        )

        val jacksonJsonDeserializer = JacksonJsonDeserializer(
            OrderCreatedEvent::class.java,
            kafkaJsonMapper,
            false
        ).apply {
            addTrustedPackages("com.example.dsf.commons")
        }

        val errorHandlingValueDeserializer = ErrorHandlingDeserializer(jacksonJsonDeserializer)

        return DefaultKafkaConsumerFactory(
            configProps,
            StringDeserializer(),
            errorHandlingValueDeserializer
        )
    }

    @Bean
    fun kafkaListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, OrderCreatedEvent>,
        errorHandler: DefaultErrorHandler
    ): ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent>()
        factory.setConsumerFactory(consumerFactory)
        factory.setCommonErrorHandler(errorHandler)
        return factory
    }

    @EventListener(ApplicationReadyEvent::class)
    fun createTopicsOnStartup() {
        AdminClient.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers)).use { admin ->
            val topics = listOf(
                NewTopic(ORDER_EVENTS_TOPIC, NUMBER_PARTITIONS, NUMBER_REPLICAS),
                NewTopic(ORDER_EVENTS_DLT_TOPIC, NUMBER_PARTITIONS, NUMBER_REPLICAS)
            )

            val existing = admin.listTopics().names().get()
            val toCreate = topics.filter { !existing.contains(it.name()) }

            if (toCreate.isNotEmpty()) {
                try {
                    admin.createTopics(toCreate).all().get()
                    logger.info { "Kafka topics created successfully on startup." }
                } catch (e: java.util.concurrent.ExecutionException) {
                    if (e.cause !is TopicExistsException) throw e
                }
            }
        }
    }

    companion object {
        const val NUMBER_PARTITIONS: Int = 3
        const val NUMBER_REPLICAS: Short = 1
        const val ORDER_EVENTS_TOPIC: String = "order-events"
        const val ORDER_EVENTS_DLT_TOPIC: String = "order-events.DLT"
    }
}