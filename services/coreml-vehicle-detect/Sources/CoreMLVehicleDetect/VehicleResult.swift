import Foundation

struct VehicleTopLabel: Codable {
    let label: String
    let confidence: Double
}

struct VehicleDetectionResult: Codable {
    let ts: Int64
    let imagePath: String
    let top: [VehicleTopLabel]

    enum CodingKeys: String, CodingKey {
        case ts
        case imagePath = "image_path"
        case top
    }
}
