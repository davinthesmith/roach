import ArgumentParser
import Foundation
import CreateML
import CoreML

struct TrainCommand: ParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "train",
        abstract: "Train a person classification model from labeled images"
    )

    @Option(name: .long, help: "Training data directory (default: TRAIN_DIR env or ./data/train)")
    var trainDir: String?

    @Option(name: .long, help: "Model output directory (default: MODEL_DIR env or ./data/models/detect-person)")
    var modelDir: String?

    @Option(name: .long, help: "Maximum training iterations")
    var maxIterations: Int?

    func run() throws {
        let config = Config.load()
        let trainURL = URL(fileURLWithPath: trainDir ?? config.trainDir)
        let modelDirURL = URL(fileURLWithPath: modelDir ?? config.modelDir)

        guard FileManager.default.fileExists(atPath: trainURL.path) else {
            print("Error: Training directory not found: \(trainURL.path)")
            print("Create it with subdirectories per person, e.g.:")
            print("  data/train/john_doe/img001.jpg")
            print("  data/train/jane_doe/img001.jpg")
            throw ExitCode.failure
        }

        let subdirs = try FileManager.default.contentsOfDirectory(
            at: trainURL,
            includingPropertiesForKeys: [.isDirectoryKey]
        ).filter { url in
            (try? url.resourceValues(forKeys: [.isDirectoryKey]).isDirectory) == true
        }

        guard !subdirs.isEmpty else {
            print("Error: No person subdirectories found in \(trainURL.path)")
            throw ExitCode.failure
        }

        print("Training data: \(trainURL.path)")
        var totalImages = 0
        for dir in subdirs.sorted(by: { $0.lastPathComponent < $1.lastPathComponent }) {
            let images = try FileManager.default.contentsOfDirectory(
                at: dir,
                includingPropertiesForKeys: nil
            ).filter { ["jpg", "jpeg", "png"].contains($0.pathExtension.lowercased()) }
            totalImages += images.count
            print("  \(dir.lastPathComponent): \(images.count) images")
        }
        print("  Total: \(subdirs.count) people, \(totalImages) images\n")

        print("Training image classifier...")
        var params = MLImageClassifier.ModelParameters()
        if let maxIterations {
            params.maxIterations = maxIterations
        }

        let classifier = try MLImageClassifier(
            trainingData: .labeledDirectories(at: trainURL),
            parameters: params
        )

        let error = classifier.trainingMetrics.classificationError
        print("Training complete!")
        print("  Classification error: \(String(format: "%.4f", error))")
        print("  Accuracy: \(String(format: "%.1f%%", (1.0 - error) * 100))")

        try FileManager.default.createDirectory(at: modelDirURL, withIntermediateDirectories: true)

        let mlmodelURL = modelDirURL.appendingPathComponent("PersonClassifier.mlmodel")
        try classifier.write(to: mlmodelURL)
        print("\nSaved .mlmodel to \(mlmodelURL.path)")

        print("Compiling model for CoreML...")
        let compiledURL = try MLModel.compileModel(at: mlmodelURL)
        let destURL = modelDirURL.appendingPathComponent("PersonClassifier.mlmodelc")

        if FileManager.default.fileExists(atPath: destURL.path) {
            try FileManager.default.removeItem(at: destURL)
        }
        try FileManager.default.moveItem(at: compiledURL, to: destURL)

        try FileManager.default.removeItem(at: mlmodelURL)

        print("Compiled model saved to \(destURL.path)")
        print("\nReady for detection: swift run DetectPerson detect")
    }
}
