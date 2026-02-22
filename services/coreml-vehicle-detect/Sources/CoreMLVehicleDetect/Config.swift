import Foundation

struct Config {
    let watchDir: String
    let carModelPath: String
    let kafkaBroker: String
    let kafkaTopic: String
    let debounceInterval: TimeInterval
    let logLevel: String
    let topN: Int

    static func load() -> Config {
        Config(
            watchDir: env("WATCH_DIR", default: "./data/streams/coreml/vehicle"),
            carModelPath: env("CAR_MODEL_PATH", default: "./data/models/CarRecognition.mlmodel"),
            kafkaBroker: env("KAFKA_BROKER", default: "localhost:9092"),
            kafkaTopic: env("KAFKA_TOPIC", default: "detect.vehicle"),
            debounceInterval: Double(env("DEBOUNCE_SECONDS", default: "1.0")) ?? 1.0,
            logLevel: env("LOG_LEVEL", default: "info"),
            topN: Int(env("TOP_N", default: "5")) ?? 5
        )
    }

    private static func env(_ key: String, default defaultValue: String) -> String {
        ProcessInfo.processInfo.environment[key] ?? defaultValue
    }
}
