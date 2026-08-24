/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
    includeBuild("build-logic")
}

val nereusM3DevelopmentRepository = providers.gradleProperty("nereusM3DevelopmentRepository")
val nereusM3DevelopmentVersion = providers.gradleProperty("nereusM3DevelopmentVersion")
val nereusM3DevelopmentEnabled =
    nereusM3DevelopmentRepository.isPresent && nereusM3DevelopmentVersion.isPresent
require(nereusM3DevelopmentRepository.isPresent == nereusM3DevelopmentVersion.isPresent) {
    "nereusM3DevelopmentRepository and nereusM3DevelopmentVersion must be provided together"
}

dependencyResolutionManagement {
    val nereusN1SourceCommit = "330aaec349c51fb2ace52b1085e8a9e5a60b5e3e"
    val nereusN1Version = "0.2.0-n1.$nereusN1SourceCommit"
    val nereusN1Repository = file("gradle/locked-artifacts/nereus-n1/$nereusN1SourceCommit/m2")
    require(nereusN1Repository.isDirectory) {
        "Missing immutable Nereus N1 repository $nereusN1Repository"
    }
    val nereusP1SourceCommit = "17497c37901288d60d5d221e73f672a314371a45"
    val nereusP1Version = "0.2.0-p1.$nereusP1SourceCommit"
    val nereusP1Repository = file("gradle/locked-artifacts/nereus-p1/$nereusP1SourceCommit/m2")
    require(nereusP1Repository.isDirectory) {
        "Missing immutable Nereus P1 repository $nereusP1Repository"
    }
    val oxiaClientSourceCommit = "091a42c2780d92da56e9ec1f02ce1c3d988adc16"
    val oxiaClientVersion = "0.9.4"
    val oxiaClientRepository = file("gradle/locked-artifacts/oxia-client-java/$oxiaClientSourceCommit/m2")
    require(oxiaClientRepository.isDirectory) {
        "Missing immutable Oxia client repository $oxiaClientRepository"
    }
    @Suppress("UnstableApiUsage")
    repositories {
        maven {
            name = "nereusN1"
            url = uri(nereusN1Repository)
            content {
                includeVersion("com.nereusstream", "nereus-domain", nereusN1Version)
                includeVersion("com.nereusstream", "nereus-metadata-spi", nereusN1Version)
            }
        }
        exclusiveContent {
            forRepository {
                maven {
                    name = "nereusP1"
                    url = uri(nereusP1Repository)
                }
            }
            filter {
                includeVersion("com.nereusstream", "nereus-metadata-oxia-p1", nereusP1Version)
            }
        }
        exclusiveContent {
            forRepository {
                maven {
                    name = "oxiaClientO1"
                    url = uri(oxiaClientRepository)
                }
            }
            filter {
                includeVersion("io.github.oxia-db", "oxia-client", oxiaClientVersion)
                includeVersion("io.github.oxia-db", "oxia-client-api", oxiaClientVersion)
            }
        }
        if (nereusM3DevelopmentEnabled) {
            maven {
                name = "nereusM3Development"
                url = uri(file(nereusM3DevelopmentRepository.get()))
                content {
                    includeVersion("com.nereusstream", "nereus-domain", nereusM3DevelopmentVersion.get())
                    includeVersion("com.nereusstream", "nereus-metadata-spi", nereusM3DevelopmentVersion.get())
                    includeVersion("com.nereusstream", "nereus-storage-api", nereusM3DevelopmentVersion.get())
                    includeVersion("com.nereusstream", "nereus-storage-object", nereusM3DevelopmentVersion.get())
                    includeVersion("com.nereusstream", "nereus-pulsar-offload", nereusM3DevelopmentVersion.get())
                }
            }
        }
        mavenCentral()
        maven {
            url = uri("https://packages.confluent.io/maven/")
            content {
                includeGroupByRegex("io\\.confluent(\\..*)?")
            }
        }
    }

    // override docker-jdk version with -PdockerJavaVersion=21|25
    val overrideDockerJavaVersion = settings.providers.gradleProperty("dockerJavaVersion")
    if (overrideDockerJavaVersion.isPresent) {
        versionCatalogs {
            create("libs") {
                version("docker-jdk", overrideDockerJavaVersion.get())
            }
        }
    }
}

rootProject.name = "pulsar"

// Running this build requires Java 21 or 25. Version check can be skipped with -PskipJavaVersionCheck parameter.
val javaVersion = providers.provider { JavaVersion.current() }
val statisfiedJavaVersion = javaVersion.map { it == JavaVersion.VERSION_21 || it == JavaVersion.VERSION_25 }
require(providers.gradleProperty("skipJavaVersionCheck").isPresent || statisfiedJavaVersion.get()) {
    "This build requires Java 21 or 25, but is running on Java ${javaVersion.get()}. Pass -PskipJavaVersionCheck to skip this check."
}

// ──────────────────────────────────────────────────────────────────────────────
// Core modules
// ──────────────────────────────────────────────────────────────────────────────

// Enforced platform for dependency version management (Maven dependencyManagement equivalent)
include("pulsar-dependencies")
// BOM for external consumers to align Pulsar module versions
include("pulsar-bom")

// Tier 0 — no internal dependencies
include("buildtools")
include("pulsar-config-validation")
include("pulsar-client-api")
include("pulsar-client-api-v5")

// Tier 1
include("pulsar-client-admin-api")
include("testmocks")

// Tier 2
include("pulsar-common")

// Tier 3
include("pulsar-cli-utils")
// Maven artifactId is "pulsar-client-original" (directory is "pulsar-client")
include("pulsar-client-original")
project(":pulsar-client-original").projectDir = file("pulsar-client")
include("pulsar-metadata")
include("pulsar-opentelemetry")
include("pulsar-client-messagecrypto-bc")
include("pulsar-client-v5")


// Tier 4
// Maven artifactId is "pulsar-client-admin-original" (directory is "pulsar-client-admin")
include("pulsar-client-admin-original")
project(":pulsar-client-admin-original").projectDir = file("pulsar-client-admin")
include("managed-ledger")
include("pulsar-broker-common")

// Tier 5 — functions core (Maven core-modules profile)
include("pulsar-functions:pulsar-functions-proto")
project(":pulsar-functions:pulsar-functions-proto").projectDir = file("pulsar-functions/proto")

include("pulsar-functions:pulsar-functions-api")
project(":pulsar-functions:pulsar-functions-api").projectDir = file("pulsar-functions/api-java")

include("pulsar-functions:pulsar-functions-utils")
project(":pulsar-functions:pulsar-functions-utils").projectDir = file("pulsar-functions/utils")

include("pulsar-functions:pulsar-functions-instance")
project(":pulsar-functions:pulsar-functions-instance").projectDir = file("pulsar-functions/instance")

include("pulsar-functions:pulsar-functions-secrets")
project(":pulsar-functions:pulsar-functions-secrets").projectDir = file("pulsar-functions/secrets")

include("pulsar-functions:pulsar-functions-runtime")
project(":pulsar-functions:pulsar-functions-runtime").projectDir = file("pulsar-functions/runtime")

include("pulsar-functions:pulsar-functions-worker")
project(":pulsar-functions:pulsar-functions-worker").projectDir = file("pulsar-functions/worker")

// Maven artifactId is "pulsar-functions-local-runner-original" (directory is "pulsar-functions/localrun")
include("pulsar-functions:pulsar-functions-local-runner-original")
project(":pulsar-functions:pulsar-functions-local-runner-original").projectDir = file("pulsar-functions/localrun")

include("pulsar-functions:pulsar-functions-api-examples")
project(":pulsar-functions:pulsar-functions-api-examples").projectDir = file("pulsar-functions/java-examples")

include("pulsar-functions:pulsar-functions-api-examples-builtin")
project(":pulsar-functions:pulsar-functions-api-examples-builtin").projectDir = file("pulsar-functions/java-examples-builtin")

include("pulsar-functions:pulsar-functions-runtime-all")
project(":pulsar-functions:pulsar-functions-runtime-all").projectDir = file("pulsar-functions/runtime-all")

// Tier 5 — transaction
include("pulsar-transaction:pulsar-transaction-common")
project(":pulsar-transaction:pulsar-transaction-common").projectDir = file("pulsar-transaction/common")

include("pulsar-transaction:pulsar-transaction-coordinator")
project(":pulsar-transaction:pulsar-transaction-coordinator").projectDir = file("pulsar-transaction/coordinator")

// Tier 5 — IO core modules (Maven core-modules profile)
include("pulsar-io:pulsar-io-core")
project(":pulsar-io:pulsar-io-core").projectDir = file("pulsar-io/core")

include("pulsar-io:pulsar-io-common")
project(":pulsar-io:pulsar-io-common").projectDir = file("pulsar-io/common")

include("pulsar-io:pulsar-io-batch-discovery-triggerers")
project(":pulsar-io:pulsar-io-batch-discovery-triggerers").projectDir = file("pulsar-io/batch-discovery-triggerers")

include("pulsar-io:pulsar-io-batch-data-generator")
project(":pulsar-io:pulsar-io-batch-data-generator").projectDir = file("pulsar-io/batch-data-generator")

include("pulsar-io:pulsar-io-data-generator")
project(":pulsar-io:pulsar-io-data-generator").projectDir = file("pulsar-io/data-generator")

// Tier 6
include("pulsar-docs-tools")

include("pulsar-package-management:pulsar-package-core")
project(":pulsar-package-management:pulsar-package-core").projectDir = file("pulsar-package-management/core")

include("pulsar-package-management:pulsar-package-filesystem-storage")
project(":pulsar-package-management:pulsar-package-filesystem-storage").projectDir = file("pulsar-package-management/filesystem-storage")

include("pulsar-websocket")
include("pulsar-broker")

include("pulsar-package-management:pulsar-package-bookkeeper-storage")
project(":pulsar-package-management:pulsar-package-bookkeeper-storage").projectDir = file("pulsar-package-management/bookkeeper-storage")

// Tier 6.5 — jetty upgrade modules
include("jetty-upgrade:pulsar-zookeeper-prometheus-metrics")
project(":jetty-upgrade:pulsar-zookeeper-prometheus-metrics").projectDir = file("jetty-upgrade/zookeeper-prometheus-metrics")
include("jetty-upgrade:zookeeper-with-patched-admin")
project(":jetty-upgrade:zookeeper-with-patched-admin").projectDir = file("jetty-upgrade/zookeeper-with-patched-admin")

// Tier 6.5 — FIPS BouncyCastle TLS integration test
include("tests:pulsar-client-test-bcfips")
project(":tests:pulsar-client-test-bcfips").projectDir = file("tests/pulsar-client-test-bcfips")

// Tier 7
include("pulsar-proxy")
include("pulsar-testclient")
include("pulsar-client-tools-api")
include("pulsar-client-tools")
include("pulsar-client-tools-test")
include("pulsar-client-tools-customcommand-example")
include("pulsar-broker-auth-oidc")
include("pulsar-broker-auth-sasl")
include("pulsar-client-auth-sasl")

// Tier 10 — shaded client modules (in core-modules)
include("pulsar-client-shaded")
include("pulsar-client-all")
include("pulsar-client-admin-shaded")

// Tier 11 — distribution (server is in core-modules)
include("distribution:pulsar-server-distribution")
project(":distribution:pulsar-server-distribution").projectDir = file("distribution/server")

// ──────────────────────────────────────────────────────────────────────────────
// Extra modules
// ──────────────────────────────────────────────────────────────────────────────

// Functions — localrun-shaded
include("pulsar-functions:pulsar-functions-local-runner-shaded")
project(":pulsar-functions:pulsar-functions-local-runner-shaded").projectDir = file("pulsar-functions/localrun-shaded")

// Tiered storage
include("jclouds-shaded")
include("tiered-storage:tiered-storage-jcloud")
project(":tiered-storage:tiered-storage-jcloud").projectDir = file("tiered-storage/jcloud")
include("tiered-storage:tiered-storage-file-system")
project(":tiered-storage:tiered-storage-file-system").projectDir = file("tiered-storage/file-system")

// Athenz auth
include("pulsar-broker-auth-athenz")
include("pulsar-client-auth-athenz")

// Distribution — extra (shell, offloaders)
include("distribution:pulsar-shell-distribution")
project(":distribution:pulsar-shell-distribution").projectDir = file("distribution/shell")
include("distribution:pulsar-offloader-distribution")
project(":distribution:pulsar-offloader-distribution").projectDir = file("distribution/offloaders")

// Misc
include("microbench")

// ──────────────────────────────────────────────────────────────────────────────
// Docker modules
// ──────────────────────────────────────────────────────────────────────────────

include("docker:pulsar-docker-image")
project(":docker:pulsar-docker-image").projectDir = file("docker/pulsar")

// Test Docker images
include("tests:java-test-functions")
project(":tests:java-test-functions").projectDir = file("tests/docker-images/java-test-functions")
include("tests:java-test-plugins")
project(":tests:java-test-plugins").projectDir = file("tests/docker-images/java-test-plugins")
include("tests:java-test-image")
project(":tests:java-test-image").projectDir = file("tests/docker-images/java-test-image")
include("tests:latest-version-image")
project(":tests:latest-version-image").projectDir = file("tests/docker-images/latest-version-image")

// ──────────────────────────────────────────────────────────────────────────────
// Integration test modules
// ──────────────────────────────────────────────────────────────────────────────

include("tests:integration")
project(":tests:integration").projectDir = file("tests/integration")

// ──────────────────────────────────────────────────────────────────────────────
// Shade test modules
// ──────────────────────────────────────────────────────────────────────────────

include("tests:pulsar-client-shade-test")
project(":tests:pulsar-client-shade-test").projectDir = file("tests/pulsar-client-shade-test")
include("tests:pulsar-client-admin-shade-test")
project(":tests:pulsar-client-admin-shade-test").projectDir = file("tests/pulsar-client-admin-shade-test")
include("tests:pulsar-client-all-shade-test")
project(":tests:pulsar-client-all-shade-test").projectDir = file("tests/pulsar-client-all-shade-test")
include("tests:pulsar-client-native-image")
project(":tests:pulsar-client-native-image").projectDir = file("tests/pulsar-client-native-image")
