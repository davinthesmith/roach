import Foundation
import CoreServices

/// Watches a directory tree for new .jpg files using macOS FSEvents.
final class FileWatcher {
    typealias Handler = (URL) -> Void

    private let watchPath: String
    private let handler: Handler
    private let debounceInterval: TimeInterval
    private var stream: FSEventStreamRef?
    private var knownFiles: Set<String> = []
    private var pendingFiles: [String: DispatchWorkItem] = [:]
    private let queue = DispatchQueue(label: "com.roach.coreml-vehicle-detect.filewatcher")

    init(watchPath: String, debounceInterval: TimeInterval = 1.0, handler: @escaping Handler) {
        self.watchPath = watchPath
        self.debounceInterval = debounceInterval
        self.handler = handler
    }

    func start() throws {
        guard FileManager.default.fileExists(atPath: watchPath) else {
            throw FileWatcherError.directoryNotFound(watchPath)
        }

        scanExistingFiles()

        var context = FSEventStreamContext()
        context.info = Unmanaged.passUnretained(self).toOpaque()

        guard let stream = FSEventStreamCreate(
            nil,
            fileWatcherCallback,
            &context,
            [watchPath] as CFArray,
            FSEventStreamEventId(kFSEventStreamEventIdSinceNow),
            0.5,
            UInt32(kFSEventStreamCreateFlagFileEvents | kFSEventStreamCreateFlagUseCFTypes)
        ) else {
            throw FileWatcherError.streamCreationFailed
        }

        self.stream = stream
        FSEventStreamSetDispatchQueue(stream, queue)
        FSEventStreamStart(stream)

        print("Watching \(watchPath) for new .jpg files (\(knownFiles.count) existing skipped)")
    }

    func stop() {
        if let stream {
            FSEventStreamStop(stream)
            FSEventStreamInvalidate(stream)
            FSEventStreamRelease(stream)
            self.stream = nil
        }
        queue.sync {
            for (_, item) in pendingFiles {
                item.cancel()
            }
            pendingFiles.removeAll()
        }
    }

    private func scanExistingFiles() {
        guard let enumerator = FileManager.default.enumerator(
            at: URL(fileURLWithPath: watchPath),
            includingPropertiesForKeys: [.isRegularFileKey]
        ) else { return }

        while let url = enumerator.nextObject() as? URL {
            if url.pathExtension.lowercased() == "jpg" {
                knownFiles.insert(url.path)
            }
        }
    }

    fileprivate func handleFileEvent(_ path: String, flags: FSEventStreamEventFlags) {
        let isFile = flags & UInt32(kFSEventStreamEventFlagItemIsFile) != 0
        let isCreated = flags & UInt32(kFSEventStreamEventFlagItemCreated) != 0
        let isModified = flags & UInt32(kFSEventStreamEventFlagItemModified) != 0
        let isRenamed = flags & UInt32(kFSEventStreamEventFlagItemRenamed) != 0

        guard isFile && (isCreated || isModified || isRenamed) else { return }
        guard path.lowercased().hasSuffix(".jpg") else { return }
        guard !knownFiles.contains(path) else { return }

        knownFiles.insert(path)

        pendingFiles[path]?.cancel()
        let workItem = DispatchWorkItem { [weak self] in
            self?.pendingFiles.removeValue(forKey: path)
            self?.handler(URL(fileURLWithPath: path))
        }
        pendingFiles[path] = workItem
        queue.asyncAfter(deadline: .now() + debounceInterval, execute: workItem)
    }
}

private func fileWatcherCallback(
    _ streamRef: ConstFSEventStreamRef,
    _ info: UnsafeMutableRawPointer?,
    _ numEvents: Int,
    _ eventPaths: UnsafeMutableRawPointer,
    _ eventFlags: UnsafePointer<FSEventStreamEventFlags>,
    _ eventIds: UnsafePointer<FSEventStreamEventId>
) {
    guard let info else { return }
    let watcher = Unmanaged<FileWatcher>.fromOpaque(info).takeUnretainedValue()
    let paths = unsafeBitCast(eventPaths, to: NSArray.self) as! [String]

    for i in 0..<numEvents {
        watcher.handleFileEvent(paths[i], flags: eventFlags[i])
    }
}

enum FileWatcherError: Error, CustomStringConvertible {
    case directoryNotFound(String)
    case streamCreationFailed

    var description: String {
        switch self {
        case .directoryNotFound(let path): return "Watch directory not found: \(path)"
        case .streamCreationFailed: return "Failed to create FSEvent stream"
        }
    }
}
