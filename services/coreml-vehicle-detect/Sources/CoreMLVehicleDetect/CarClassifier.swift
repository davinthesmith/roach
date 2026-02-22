import Foundation
import Vision
import CoreML

/// Runs CompCars-style car make/model classification via Core ML; returns top-N labels and confidences.
enum CarClassifier {
    nonisolated(unsafe) private static var sharedModel: VNCoreMLModel?

    static func loadModel(at path: String) throws -> VNCoreMLModel {
        if let m = sharedModel { return m }
        let url = URL(fileURLWithPath: path)
        guard FileManager.default.fileExists(atPath: path) else {
            throw CarClassifierError.modelNotFound(path)
        }
        let mlModel = try MLModel(contentsOf: url)
        let model = try VNCoreMLModel(for: mlModel)
        sharedModel = model
        return model
    }

    /// Classify image at url; return top-N (label, confidence). Returns nil on failure.
    static func classify(imageAt url: URL, topN: Int) throws -> [VehicleTopLabel]? {
        let data = try Data(contentsOf: url)
        guard let imageSource = CGImageSourceCreateWithData(data as CFData, nil),
              let cgImage = CGImageSourceCreateImageAtIndex(imageSource, 0, nil) else {
            throw CarClassifierError.invalidImage(url.path)
        }

        guard let model = sharedModel else {
            throw CarClassifierError.modelNotLoaded
        }

        let request = VNCoreMLRequest(model: model)
        request.imageCropAndScaleOption = .scaleFill

        let handler = VNImageRequestHandler(cgImage: cgImage, orientation: .up, options: [:])
        try handler.perform([request])

        guard let results = request.results as? [VNClassificationObservation] else {
            return nil
        }

        let top = results.prefix(topN).map { obs in
            VehicleTopLabel(label: obs.identifier, confidence: Double(obs.confidence))
        }
        return Array(top)
    }
}

enum CarClassifierError: Error, CustomStringConvertible {
    case modelNotFound(String)
    case modelNotLoaded
    case invalidImage(String)

    var description: String {
        switch self {
        case .modelNotFound(let path): return "Car model not found at \(path)"
        case .modelNotLoaded: return "Car model not loaded; call loadModel first"
        case .invalidImage(let path): return "Invalid image at \(path)"
        }
    }
}
