import Foundation
import ImageIO
import UniformTypeIdentifiers
import Vision

/// Detects faces with Vision and crops each face to a separate image.
enum FaceCrop {
    /// Run face detection on the image at `url`. For each face, crop and write to `outputDir`.
    /// Naming: one face -> `{base}.jpg`; multiple -> `{base}_0.jpg`, `{base}_1.jpg`, ...
    /// Returns the number of face crops written.
    static func process(imageAt url: URL, outputDir: String) throws -> Int {
        let data = try Data(contentsOf: url)
        guard let imageSource = CGImageSourceCreateWithData(data as CFData, nil),
              let cgImage = CGImageSourceCreateImageAtIndex(imageSource, 0, nil) else {
            throw FaceCropError.invalidImage(url.path)
        }

        let request = VNDetectFaceRectanglesRequest()
        let handler = VNImageRequestHandler(cgImage: cgImage, orientation: .up, options: [:])
        try handler.perform([request])

        let results = request.results ?? []
        guard !results.isEmpty else {
            return 0
        }

        let width = CGFloat(cgImage.width)
        let height = CGFloat(cgImage.height)
        let base = url.deletingPathExtension().lastPathComponent

        try FileManager.default.createDirectory(atPath: outputDir, withIntermediateDirectories: true)

        var written = 0
        for (index, observation) in results.enumerated() {
            let rect = imageRectFromNormalized(observation.boundingBox, imageWidth: width, imageHeight: height)
            let clampedRect = rect.intersection(CGRect(x: 0, y: 0, width: width, height: height))
            guard clampedRect.width > 0, clampedRect.height > 0 else { continue }

            guard let cropped = cgImage.cropping(to: clampedRect) else { continue }

            let name = results.count == 1 ? "\(base).jpg" : "\(base)_\(index).jpg"
            let outputURL = URL(fileURLWithPath: outputDir).appendingPathComponent(name)

            guard let dest = CGImageDestinationCreateWithURL(
                outputURL as CFURL,
                UTType.jpeg.identifier as CFString,
                1,
                nil
            ) else { continue }

            CGImageDestinationAddImage(dest, cropped, [kCGImageDestinationLossyCompressionQuality: 0.9] as CFDictionary)
            if CGImageDestinationFinalize(dest) {
                written += 1
            }
        }

        return written
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

enum FaceCropError: Error, CustomStringConvertible {
    case invalidImage(String)

    var description: String {
        switch self {
        case .invalidImage(let path): return "Invalid image at \(path)"
        }
    }
}
