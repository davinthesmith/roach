// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "CoreMLSmartCrop",
    platforms: [.macOS(.v15)],
    dependencies: [],
    targets: [
        .executableTarget(
            name: "CoreMLSmartCrop",
            dependencies: [],
            swiftSettings: [.unsafeFlags(["-parse-as-library"])]
        ),
    ]
)
