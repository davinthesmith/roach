// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "DetectPerson",
    platforms: [.macOS(.v15)],
    dependencies: [
        .package(url: "https://github.com/apple/swift-argument-parser", from: "1.3.0"),
        .package(url: "https://github.com/swift-server/swift-kafka-client", branch: "main"),
        .package(url: "https://github.com/apple/swift-log", from: "1.5.0"),
        .package(url: "https://github.com/swift-server/swift-service-lifecycle", from: "2.0.0"),
    ],
    targets: [
        .executableTarget(
            name: "DetectPerson",
            dependencies: [
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
                .product(name: "Kafka", package: "swift-kafka-client"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "ServiceLifecycle", package: "swift-service-lifecycle"),
            ]
        ),
    ]
)
