import ArgumentParser
import Foundation
import Logging
import ServiceLifecycle

struct DetectCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "detect",
        abstract: "Watch for new images and classify detected people"
    )

    @Option(name: .long, help: "Override watch directory")
    var watchDir: String?

    func run() async throws {
        let config = Config.load()
        var logger = Logger(label: "detect-person")
        logger.logLevel = config.logLevel == "debug" ? .debug : .info

        let modelURL = config.compiledModelURL
        logger.info("Loading model from \(modelURL.path)")
        let classifier = try PersonClassifier(
            modelURL: modelURL,
            confidenceThreshold: config.confidenceThreshold,
            maxAlternatives: config.maxAlternatives
        )
        logger.info("Model loaded (threshold: \(config.confidenceThreshold))")

        logger.info("Connecting to Kafka broker at \(config.kafkaBroker)")
        let producer = try DetectionProducer(
            broker: config.kafkaBroker,
            topic: config.kafkaTopic,
            logger: logger
        )
        logger.info("Kafka producer ready (topic: \(config.kafkaTopic))")

        let effectiveWatchDir = watchDir ?? config.watchDir
        let taskLogger = logger

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Kafka producer lifecycle
            group.addTask {
                let serviceGroup = ServiceGroup(
                    services: [producer.kafkaProducer],
                    gracefulShutdownSignals: [.sigterm, .sigint],
                    cancellationSignals: [],
                    logger: taskLogger
                )
                try await serviceGroup.run()
            }

            // Drain delivery reports to prevent backpressure
            group.addTask {
                for await event in producer.events {
                    switch event {
                    case .deliveryReports(let reports):
                        for report in reports {
                            if case .failure(let error) = report.status {
                                taskLogger.error("Kafka delivery failed: \(error)")
                            }
                        }
                    default:
                        break
                    }
                }
            }

            // Run file watcher and classification pipeline
            group.addTask {
                let watcher = FileWatcher(
                    watchPath: effectiveWatchDir,
                    debounceInterval: config.debounceInterval
                ) { url in
                    do {
                        if let result = try classifier.classify(imageAt: url) {
                            let headers: [(String, String)] = [
                                ("schema_version", "1"),
                                ("camera_name", result.cameraName),
                                ("event_start", String(result.eventStart)),
                                ("timestamp", String(result.imageTimestamp)),
                                ("source", "detect-person"),
                            ]
                            try producer.send(result: result, headers: headers)
                            taskLogger.info("\(result.person) (\(String(format: "%.0f%%", result.confidence * 100))) — \(result.imagePath)")
                        } else {
                            taskLogger.debug("No match above threshold for \(url.lastPathComponent)")
                        }
                    } catch {
                        taskLogger.error("Classification error for \(url.lastPathComponent): \(error)")
                    }
                }
                try watcher.start()

                while !Task.isCancelled {
                    try await Task.sleep(nanoseconds: 1_000_000_000)
                }
                watcher.stop()
            }

            // Wait for any task to finish (shutdown signal cancels the group)
            try await group.next()
            group.cancelAll()
        }

        logger.info("detect-person stopped")
    }
}
