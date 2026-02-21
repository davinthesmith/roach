import ArgumentParser

@main
struct DetectPerson: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Person detection and classification using CoreML",
        discussion: """
            Train a model from labeled images, then detect and classify people
            in UniFi Protect smart archive images. Detections are published to Kafka.
            """,
        subcommands: [TrainCommand.self, DetectCommand.self]
    )
}
