import Foundation
import Logging
import Darwin

@main
struct Main {
    static func main() {
        let config = Config.load()
        let isDebug = config.logLevel == "debug"
        var logger = Logger(label: "coreml-vehicle-detect")
        logger.logLevel = isDebug ? .debug : .info

        while true {
            do {
                _ = try CarClassifier.loadModel(at: config.carModelPath)
                break
            } catch {
                print("Car model not ready: \(error). Retrying in 10s...")
                if let carErr = error as? CarClassifierError, case .modelNotFound = carErr {
                    print("Run ./scripts/models/download-car-model.sh to download the model.")
                }
                fflush(stdout)
                Thread.sleep(forTimeInterval: 10)
            }
        }

        let producer: VehicleKafkaProducer
        do {
            producer = try VehicleKafkaProducer(
                broker: config.kafkaBroker,
                topic: config.kafkaTopic,
                logger: logger
            )
        } catch {
            print("Failed to create Kafka producer: \(error)")
            exit(1)
        }

        print("coreml-vehicle-detect starting (watch: \(config.watchDir), topic: \(config.kafkaTopic))")
        fflush(stdout)

        try? FileManager.default.createDirectory(atPath: config.watchDir, withIntermediateDirectories: true)

        let watcher = FileWatcher(
            watchPath: config.watchDir,
            debounceInterval: config.debounceInterval
        ) { url in
            let imageTimestamp = url.deletingPathExtension().lastPathComponent
            let ts = Int64(imageTimestamp) ?? Int64(Date().timeIntervalSince1970)

            do {
                guard let top = try CarClassifier.classify(imageAt: url, topN: config.topN), !top.isEmpty else {
                    if isDebug { print("No classification for \(url.lastPathComponent)") }
                    return
                }

                let result = VehicleDetectionResult(
                    ts: ts,
                    imagePath: url.path,
                    top: top
                )
                let key = "vehicle:\(imageTimestamp)"
                try producer.send(result: result, key: key)
                print("Published \(url.lastPathComponent) -> \(config.kafkaTopic) (\(top[0].label))")
            } catch {
                print("Error processing \(url.lastPathComponent): \(error)")
            }
        }

        do {
            try watcher.start()
        } catch {
            print("Failed to start watcher: \(error)")
            exit(1)
        }

        let sem = DispatchSemaphore(value: 0)
        let sigSrc = DispatchSource.makeSignalSource(signal: SIGINT, queue: .main)
        let sigTerm = DispatchSource.makeSignalSource(signal: SIGTERM, queue: .main)
        sigSrc.setEventHandler { watcher.stop(); sem.signal() }
        sigTerm.setEventHandler { watcher.stop(); sem.signal() }
        sigSrc.resume()
        sigTerm.resume()

        sem.wait()
        exit(0)
    }
}
