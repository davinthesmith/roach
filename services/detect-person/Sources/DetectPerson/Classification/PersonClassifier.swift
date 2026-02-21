import CoreImage
import CoreML
import Foundation
import Vision

final class PersonClassifier: @unchecked Sendable {
    private let model: VNCoreMLModel
    private let confidenceThreshold: Double
    private let maxAlternatives: Int

    init(modelURL: URL, confidenceThreshold: Double, maxAlternatives: Int) throws {
        guard FileManager.default.fileExists(atPath: modelURL.path) else {
            throw ClassifierError.modelNotFound(modelURL.path)
        }
        let mlModel = try MLModel(contentsOf: modelURL)
        self.model = try VNCoreMLModel(for: mlModel)
        self.confidenceThreshold = confidenceThreshold
        self.maxAlternatives = maxAlternatives
    }

    /// Classify the person in the image. Returns nil if no match exceeds the confidence threshold.
    func classify(imageAt url: URL) throws -> DetectionResult? {
        guard let imageData = try? Data(contentsOf: url),
              let ciImage = CIImage(data: imageData) else {
            throw ClassifierError.invalidImage(url.path)
        }

        var observations: [VNClassificationObservation] = []
        var requestError: Error?
        let semaphore = DispatchSemaphore(value: 0)

        let request = VNCoreMLRequest(model: model) { request, error in
            if let error {
                requestError = error
            } else if let results = request.results as? [VNClassificationObservation] {
                observations = results
            }
            semaphore.signal()
        }

        let handler = VNImageRequestHandler(ciImage: ciImage)
        try handler.perform([request])
        semaphore.wait()

        if let error = requestError {
            throw error
        }

        guard let top = observations.first,
              Double(top.confidence) >= confidenceThreshold else {
            return nil
        }

        let metadata = ImageMetadata.parse(from: url)

        let alternatives = Array(observations.dropFirst().prefix(maxAlternatives)).map {
            PersonMatch(person: $0.identifier, confidence: Double($0.confidence))
        }

        return DetectionResult(
            person: top.identifier,
            confidence: Double(top.confidence),
            alternatives: alternatives,
            imagePath: metadata.relativePath,
            cameraName: metadata.cameraName,
            eventStart: metadata.eventStart,
            imageTimestamp: metadata.imageTimestamp
        )
    }
}

enum ClassifierError: Error, CustomStringConvertible {
    case modelNotFound(String)
    case invalidImage(String)

    var description: String {
        switch self {
        case .modelNotFound(let path): return "Model not found at \(path)"
        case .invalidImage(let path): return "Could not load image at \(path)"
        }
    }
}
