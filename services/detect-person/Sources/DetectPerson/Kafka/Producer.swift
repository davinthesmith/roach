import Foundation
import Kafka
import Logging
import NIOCore
import ServiceLifecycle

/// Wraps KafkaProducer for publishing detection results.
final class DetectionProducer: Sendable {
    let kafkaProducer: KafkaProducer
    let events: KafkaProducerEvents
    private let topic: String
    private let logger: Logger

    init(broker: String, topic: String, logger: Logger) throws {
        self.topic = topic
        self.logger = logger

        let parts = broker.split(separator: ":")
        let host = String(parts[0])
        let port = parts.count > 1 ? Int(parts[1]) ?? 9092 : 9092

        let brokerAddress = KafkaConfiguration.BrokerAddress(host: host, port: port)
        let configuration = KafkaProducerConfiguration(bootstrapBrokerAddresses: [brokerAddress])

        let (producer, events) = try KafkaProducer.makeProducerWithEvents(
            configuration: configuration,
            logger: logger
        )
        self.kafkaProducer = producer
        self.events = events
    }

    /// Publish a detection result to Kafka. Returns the message ID for tracking delivery.
    @discardableResult
    func send(result: DetectionResult, headers: [(String, String)]) throws -> KafkaProducerMessageID {
        let encoder = JSONEncoder()
        let jsonData = try encoder.encode(result)

        let key = "\(result.person):\(result.imageTimestamp)"

        let kafkaHeaders = headers.map {
            KafkaHeader(key: $0.0, value: ByteBuffer(string: $0.1))
        }

        var valueBuffer = ByteBufferAllocator().buffer(capacity: jsonData.count)
        valueBuffer.writeBytes(jsonData)

        let message = KafkaProducerMessage(
            topic: topic,
            headers: kafkaHeaders,
            key: ByteBuffer(string: key),
            value: valueBuffer
        )

        return try kafkaProducer.send(message)
    }
}
