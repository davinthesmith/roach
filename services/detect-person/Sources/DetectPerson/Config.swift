import Foundation

struct Config {
    let kafkaBroker: String
    let kafkaTopic: String
    let watchDir: String
    let trainDir: String
    let modelDir: String
    let confidenceThreshold: Double
    let maxAlternatives: Int
    let logLevel: String
    let debounceInterval: TimeInterval

    static func load() -> Config {
        Config(
            kafkaBroker: env("KAFKA_BROKER", default: "localhost:9092"),
            kafkaTopic: env("KAFKA_TOPIC", default: "detect.person"),
            watchDir: env("WATCH_DIR", default: "./data/streams/unifi/protect/smart/person"),
            trainDir: env("TRAIN_DIR", default: "./data/train"),
            modelDir: env("MODEL_DIR", default: "./data/models/detect-person"),
            confidenceThreshold: Double(env("CONFIDENCE_THRESHOLD", default: "0.7")) ?? 0.7,
            maxAlternatives: Int(env("MAX_ALTERNATIVES", default: "5")) ?? 5,
            logLevel: env("LOG_LEVEL", default: "info"),
            debounceInterval: Double(env("DEBOUNCE_SECONDS", default: "1.0")) ?? 1.0
        )
    }

    private static func env(_ key: String, default defaultValue: String) -> String {
        ProcessInfo.processInfo.environment[key] ?? defaultValue
    }

    var compiledModelURL: URL {
        URL(fileURLWithPath: modelDir).appendingPathComponent("PersonClassifier.mlmodelc")
    }

    var mlmodelURL: URL {
        URL(fileURLWithPath: modelDir).appendingPathComponent("PersonClassifier.mlmodel")
    }

    var modelDirURL: URL {
        URL(fileURLWithPath: modelDir)
    }

    var trainDirURL: URL {
        URL(fileURLWithPath: trainDir)
    }

    var watchDirURL: URL {
        URL(fileURLWithPath: watchDir)
    }
}
