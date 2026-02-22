import Foundation
import Kafka
import Logging
import NIOCore

/// Sends vehicle detection results to Kafka.
final class VehicleKafkaProducer {
    let kafkaProducer: KafkaProducer
    private let topic: String

    init(broker: String, topic: String, logger: Logger) throws {
        self.topic = topic

        let parts = broker.split(separator: ":")
        let host = String(parts[0])
        let port = parts.count > 1 ? Int(parts[1]) ?? 9092 : 9092

        let brokerAddress = KafkaConfiguration.BrokerAddress(host: host, port: port)
        let configuration = KafkaProducerConfiguration(bootstrapBrokerAddresses: [brokerAddress])

        let (producer, _) = try KafkaProducer.makeProducerWithEvents(
            configuration: configuration,
            logger: logger
        )
        self.kafkaProducer = producer
    }

    func send(result: VehicleDetectionResult, key: String) throws {
        let encoder = JSONEncoder()
        let jsonData = try encoder.encode(result)

        var valueBuffer = ByteBufferAllocator().buffer(capacity: jsonData.count)
        valueBuffer.writeBytes(jsonData)

        let message = KafkaProducerMessage(
            topic: topic,
            headers: [],
            key: ByteBuffer(string: key),
            value: valueBuffer
        )

        _ = try kafkaProducer.send(message)
    }
}
