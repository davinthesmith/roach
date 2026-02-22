// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "CoreMLFaceCrop",
    platforms: [.macOS(.v15)],
    dependencies: [],
    targets: [
        .executableTarget(
            name: "CoreMLFaceCrop",
            dependencies: [],
            swiftSettings: [.unsafeFlags(["-parse-as-library"])]
        ),
    ]
)
