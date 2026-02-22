import Foundation

@main
struct Main {
    static func main() {
        let config = Config.load()
        let isDebug = config.logLevel == "debug"

        func log(_ message: String) {
            print(message)
        }
        func logDebug(_ message: String) {
            if isDebug { print(message) }
        }

        log("coreml-face-crop starting (watch: \(config.watchDir), output: \(config.facesDir))")

        try? FileManager.default.createDirectory(atPath: config.watchDir, withIntermediateDirectories: true)
        try? FileManager.default.createDirectory(atPath: config.facesDir, withIntermediateDirectories: true)

        let watcher = FileWatcher(
            watchPath: config.watchDir,
            debounceInterval: config.debounceInterval
        ) { url in
            do {
                let count = try FaceCrop.process(imageAt: url, outputDir: config.facesDir)
                if count > 0 {
                    log("Faces from \(url.lastPathComponent) -> \(config.facesDir) (\(count) crop(s))")
                } else {
                    logDebug("No face detected in \(url.lastPathComponent)")
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
