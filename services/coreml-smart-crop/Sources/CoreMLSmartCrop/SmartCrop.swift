import Foundation
import ImageIO
import UniformTypeIdentifiers
import Vision
import CoreML

/// Event types we support; must match path segment under smart/.
enum SmartCropEventType: String, CaseIterable {
    case person
    case package
    case animal
    case vehicle
}

/// COCO-style class allowlists per event type. Labels from YOLO are typically lowercase.
private let allowlist: [SmartCropEventType: Set<String>] = [
    .person: ["person"],
    .vehicle: ["car", "truck", "bus", "motorcycle"],
    .animal: ["dog", "cat", "horse", "sheep", "cow", "elephant", "bear", "zebra", "giraffe", "bird"],
    .package: ["backpack", "handbag", "suitcase"], // COCO has no "package"; use bag-like classes
]

/// Detects the best-matching object with a YOLO Core ML model, crops to that box, and writes to output.
enum SmartCrop {
    nonisolated(unsafe) private static var sharedModel: VNCoreMLModel?

    /// Load YOLO model once. Call from main before process.
    /// If path is .mlpackage, compiles to .mlmodelc on first use (same pattern as coreml-vehicle-detect).
    static func loadModel(at path: String) throws -> VNCoreMLModel {
        if let m = sharedModel { return m }
        let pathResolved = (path as NSString).standardizingPath
        let url = URL(fileURLWithPath: pathResolved)
        guard FileManager.default.fileExists(atPath: pathResolved) else {
            throw SmartCropError.modelNotFound(pathResolved)
        }
        let modelURL: URL
        if url.pathExtension == "mlpackage" {
            let compiledPath = (pathResolved as NSString).deletingPathExtension + ".mlmodelc"
            if FileManager.default.fileExists(atPath: compiledPath) {
                modelURL = URL(fileURLWithPath: compiledPath)
            } else {
                let compiledURL = try MLModel.compileModel(at: url)
                let destURL = URL(fileURLWithPath: compiledPath)
                if FileManager.default.fileExists(atPath: destURL.path) {
                    try FileManager.default.removeItem(at: destURL)
                }
                try FileManager.default.moveItem(at: compiledURL, to: destURL)
                modelURL = destURL
            }
        } else {
            modelURL = url
        }
        let mlModel = try MLModel(contentsOf: modelURL)
        let model = try VNCoreMLModel(for: mlModel)
        sharedModel = model
        return model
    }

    /// Run object detection, filter by allowlist for `eventType`, pick best detection, crop, write.
    /// Returns true if a crop was written.
    static func process(imageAt url: URL, eventType: SmartCropEventType, outputDir: String) throws -> Bool {
        let data = try Data(contentsOf: url)
        guard let imageSource = CGImageSourceCreateWithData(data as CFData, nil),
              let cgImage = CGImageSourceCreateImageAtIndex(imageSource, 0, nil) else {
            throw SmartCropError.invalidImage(url.path)
        }

        guard let model = sharedModel else {
            throw SmartCropError.modelNotLoaded
        }

        let request = VNCoreMLRequest(model: model)
        request.imageCropAndScaleOption = .scaleFill

        let handler = VNImageRequestHandler(cgImage: cgImage, orientation: .up, options: [:])
        try handler.perform([request])

        guard let results = request.results as? [VNRecognizedObjectObservation] else {
            return false
        }

        let allowed = allowlist[eventType] ?? []
        let width = CGFloat(cgImage.width)
        let height = CGFloat(cgImage.height)
        let imageRect = CGRect(x: 0, y: 0, width: width, height: height)

        var best: (observation: VNRecognizedObjectObservation, confidence: Float)?
        for obs in results {
            let label = obs.labels.first?.identifier.lowercased() ?? ""
            guard allowed.contains(label) else { continue }
            let conf = obs.labels.first?.confidence ?? 0
            if best == nil || conf > best!.confidence {
                best = (obs, conf)
            }
        }

        guard let b = best else { return false }

        let rect = imageRectFromNormalized(b.observation.boundingBox, imageWidth: width, imageHeight: height)
        let clampedRect = rect.intersection(imageRect)
        guard clampedRect.width > 0, clampedRect.height > 0 else { return false }

        guard let cropped = cgImage.cropping(to: clampedRect) else {
            throw SmartCropError.cropFailed
        }

        let imageTimestamp = url.deletingPathExtension().lastPathComponent
        let outputURL = URL(fileURLWithPath: outputDir).appendingPathComponent("\(imageTimestamp).jpg")

        try FileManager.default.createDirectory(atPath: outputDir, withIntermediateDirectories: true)

        guard let dest = CGImageDestinationCreateWithURL(
            outputURL as CFURL,
            UTType.jpeg.identifier as CFString,
            1,
            nil
        ) else {
            throw SmartCropError.writeFailed(outputURL.path)
        }

        CGImageDestinationAddImage(dest, cropped, [kCGImageDestinationLossyCompressionQuality: 0.9] as CFDictionary)
        guard CGImageDestinationFinalize(dest) else {
            throw SmartCropError.writeFailed(outputURL.path)
        }

        return true
    }

    /// Convert Vision normalized rect (origin lower-left, 0-1) to image pixel rect (origin upper-left).
    private static func imageRectFromNormalized(_ normalized: CGRect, imageWidth: CGFloat, imageHeight: CGFloat) -> CGRect {
        let x = normalized.minX * imageWidth
        let y = (1.0 - normalized.maxY) * imageHeight
        let w = normalized.width * imageWidth
        let h = normalized.height * imageHeight
        return CGRect(x: x, y: y, width: w, height: h)
    }
}

enum SmartCropError: Error, CustomStringConvertible {
    case modelNotFound(String)
    case modelNotLoaded
    case invalidImage(String)
    case cropFailed
    case writeFailed(String)

    var description: String {
        switch self {
        case .modelNotFound(let path): return "YOLO model not found at \(path)"
        case .modelNotLoaded: return "YOLO model not loaded; call loadModel first"
        case .invalidImage(let path): return "Invalid image at \(path)"
        case .cropFailed: return "Failed to crop image"
        case .writeFailed(let path): return "Failed to write \(path)"
        }
    }
}
