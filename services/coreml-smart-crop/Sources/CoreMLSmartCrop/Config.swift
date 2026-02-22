import Foundation

struct Config {
    let watchRoot: String
    let coremlOutputDir: String
    let yoloModelPath: String
    let debounceInterval: TimeInterval
    let logLevel: String

    static func load() -> Config {
        Config(
            watchRoot: env("WATCH_ROOT", default: "./data/streams/unifi/protect/smart"),
            coremlOutputDir: env("COREML_OUTPUT_DIR", default: "./data/streams/coreml"),
            yoloModelPath: env("YOLO_MODEL_PATH", default: "./models/yolo.mlpackage"),
            debounceInterval: Double(env("DEBOUNCE_SECONDS", default: "1.0")) ?? 1.0,
            logLevel: env("LOG_LEVEL", default: "info")
        )
    }

    private static func env(_ key: String, default defaultValue: String) -> String {
        ProcessInfo.processInfo.environment[key] ?? defaultValue
    }

    /// Output directory for a given event type (e.g. person, vehicle). Writes to coreml/{type}/.
    func outputDir(for type: String) -> String {
        (coremlOutputDir as NSString).appendingPathComponent(type)
    }
}
