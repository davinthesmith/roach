import Foundation

@main
struct Main {
    /// Parse event type from path: .../smart/{person|package|animal|vehicle}/...
    private static func eventType(from url: URL) -> SmartCropEventType? {
        let path = url.path
        for type in SmartCropEventType.allCases {
            if path.contains("/smart/\(type.rawValue)/") {
                return type
            }
        }
        return nil
    }

    static func main() {
        let config = Config.load()
        let isDebug = config.logLevel == "debug"

        func log(_ message: String) { print(message) }
        func logDebug(_ message: String) { if isDebug { print(message) } }

        while true {
            do {
                _ = try SmartCrop.loadModel(at: config.yoloModelPath)
                break
            } catch {
                log("YOLO model not ready: \(error). Retrying in 10s...")
                Thread.sleep(forTimeInterval: 10)
            }
        }

        log("coreml-smart-crop starting (watch: \(config.watchRoot), output: \(config.coremlOutputDir))")

        try? FileManager.default.createDirectory(atPath: config.watchRoot, withIntermediateDirectories: true)
        for type in SmartCropEventType.allCases {
            try? FileManager.default.createDirectory(atPath: config.outputDir(for: type.rawValue), withIntermediateDirectories: true)
        }

        let watcher = FileWatcher(
            watchPath: config.watchRoot,
            debounceInterval: config.debounceInterval
        ) { url in
            guard let eventType = eventType(from: url) else {
                logDebug("Skipping \(url.path) (unknown event type)")
                return
            }
            let outputDir = config.outputDir(for: eventType.rawValue)
            do {
                let written = try SmartCrop.process(imageAt: url, eventType: eventType, outputDir: outputDir)
                if written {
                    log("Cropped \(url.lastPathComponent) -> \(outputDir)/\(url.deletingPathExtension().lastPathComponent).jpg")
                } else {
                    logDebug("No allowed detection in \(url.lastPathComponent) for \(eventType.rawValue)")
                }
            } catch {
                log("Error processing \(url.lastPathComponent): \(error)")
            }
        }

        do {
            try watcher.start()
        } catch {
            log("Failed to start watcher: \(error)")
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
