import Foundation
import Kafka
import Logging
import NIOCore

/// Sends vehicle detection results to Kafka.
final class VehicleKafkaProducer {
    let kafkaProducer: KafkaProducer
    private let topic: String
    /// Explicit strong reference so the events sequence is never deinited (library closes producer when it is).
    private let events: KafkaProducerEvents
    /// Keeps the producer's poll loop running so buffered messages are sent.
    private let runTask: Task<Void, Error>
    /// Consuming this keeps the events sequence alive; otherwise the library closes the producer.
    private let eventsConsumptionTask: Task<Void, Error>
    private let debug: Bool

    init(broker: String, topic: String, logger: Logger, debug: Bool = false) throws {
        self.topic = topic
        self.debug = debug

        if debug { print("[Kafka] init: broker=\(broker), topic=\(topic)") }
        fflush(stdout)

        let parts = broker.split(separator: ":")
        let host = String(parts[0])
        let port = parts.count > 1 ? Int(parts[1]) ?? 9092 : 9092

        let brokerAddress = KafkaConfiguration.BrokerAddress(host: host, port: port)
        var configuration = KafkaProducerConfiguration(bootstrapBrokerAddresses: [brokerAddress])
        configuration.isAutoCreateTopicsEnabled = true  // create topic on first produce if broker allows

        if debug { print("[Kafka] calling makeProducerWithEvents...") }
        fflush(stdout)
        let (producer, eventsSequence) = try KafkaProducer.makeProducerWithEvents(
            configuration: configuration,
            logger: logger
        )
        if debug { print("[Kafka] makeProducerWithEvents returned OK") }
        fflush(stdout)

        self.kafkaProducer = producer
        self.events = eventsSequence  // retain so sequence is never deinited
        // Both tasks must run for the producer to stay open: run() drives the poll loop; consuming events
        // prevents the library from closing the producer when the sequence would otherwise be deinited.
        if debug { print("[Kafka] starting runTask (poll loop)...") }
        fflush(stdout)
        self.runTask = Task { try await producer.run() }
        if debug { print("[Kafka] starting eventsConsumptionTask...") }
        fflush(stdout)
        self.eventsConsumptionTask = Task {
            for try await _ in eventsSequence { /* drain delivery reports */ }
        }
        if debug { print("[Kafka] init complete, both tasks scheduled") }
        fflush(stdout)
    }

    func send(result: VehicleDetectionResult, key: String) throws {
        if debug { print("[Kafka] send: topic=\(topic), key=\(key)") }
        fflush(stdout)

        let encoder = JSONEncoder()
        let jsonData = try encoder.encode(result)
        let payloadSize = jsonData.count
        if debug { print("[Kafka] send: payload size=\(payloadSize) bytes") }
        fflush(stdout)

        var valueBuffer = ByteBufferAllocator().buffer(capacity: jsonData.count)
        valueBuffer.writeBytes(jsonData)

        let message = KafkaProducerMessage(
            topic: topic,
            headers: [],
            key: ByteBuffer(string: key),
            value: valueBuffer
        )

        if debug { print("[Kafka] send: calling kafkaProducer.send(...)") }
        fflush(stdout)
        do {
            _ = try kafkaProducer.send(message)
            if debug { print("[Kafka] send: kafkaProducer.send() returned OK") }
            fflush(stdout)
        } catch {
            if debug { print("[Kafka] send: kafkaProducer.send() threw: \(error)") }
            fflush(stdout)
            throw error
        }
    }
}
