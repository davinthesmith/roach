// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "CoreMLVehicleDetect",
    platforms: [.macOS(.v15)],
    dependencies: [
        .package(url: "https://github.com/swift-server/swift-kafka-client", branch: "main"),
        .package(url: "https://github.com/apple/swift-log", from: "1.5.0"),
    ],
    targets: [
        .executableTarget(
            name: "CoreMLVehicleDetect",
            dependencies: [
                .product(name: "Kafka", package: "swift-kafka-client"),
                .product(name: "Logging", package: "swift-log"),
            ],
            swiftSettings: [.unsafeFlags(["-parse-as-library"])]
        ),
    ]
)
