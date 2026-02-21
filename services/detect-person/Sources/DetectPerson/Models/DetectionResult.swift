import Foundation

struct PersonMatch: Codable {
    let person: String
    let confidence: Double
}

struct DetectionResult: Codable {
    let person: String
    let confidence: Double
    let alternatives: [PersonMatch]
    let imagePath: String
    let cameraName: String
    let eventStart: Int64
    let imageTimestamp: Int64

    enum CodingKeys: String, CodingKey {
        case person, confidence, alternatives
        case imagePath = "image_path"
        case cameraName = "camera_name"
        case eventStart = "event_start"
        case imageTimestamp = "image_timestamp"
    }
}

struct ImageMetadata {
    let relativePath: String
    let cameraName: String
    let eventStart: Int64
    let imageTimestamp: Int64

    /// Parse metadata from archive image path.
    /// Expected: .../smart/person/{camera_name}/{start_seconds}/{timestamp}.jpg
    static func parse(from url: URL) -> ImageMetadata {
        let components = url.pathComponents
        let count = components.count

        let imageTimestamp = Int64(url.deletingPathExtension().lastPathComponent) ?? 0
        let eventStart = count >= 2 ? (Int64(components[count - 2]) ?? 0) : 0
        let cameraName = count >= 3 ? components[count - 3] : "unknown"

        if let smartIndex = components.firstIndex(of: "smart") {
            let relativePath = components[smartIndex...].joined(separator: "/")
            return ImageMetadata(
                relativePath: relativePath,
                cameraName: cameraName,
                eventStart: eventStart,
                imageTimestamp: imageTimestamp
            )
        }

        return ImageMetadata(
            relativePath: url.lastPathComponent,
            cameraName: cameraName,
            eventStart: eventStart,
            imageTimestamp: imageTimestamp
        )
    }
}
