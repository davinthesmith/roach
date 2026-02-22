import Foundation

struct Config {
    let watchDir: String
    let facesDir: String
    let debounceInterval: TimeInterval
    let logLevel: String

    static func load() -> Config {
        Config(
            watchDir: env("WATCH_DIR", default: "./data/streams/coreml/person"),
            facesDir: env("FACES_DIR", default: "./data/streams/coreml/faces"),
            debounceInterval: Double(env("DEBOUNCE_SECONDS", default: "1.0")) ?? 1.0,
            logLevel: env("LOG_LEVEL", default: "info")
        )
    }

    private static func env(_ key: String, default defaultValue: String) -> String {
        ProcessInfo.processInfo.environment[key] ?? defaultValue
    }
}
