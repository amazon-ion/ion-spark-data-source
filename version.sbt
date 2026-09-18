// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

// Maven Central publication version for this package.
//
// Release tags encode Spark/Scala compatibility (e.g. v4.1_2.13) rather than the Maven
// version, so they are not a reliable version source. This value is the source of truth
// for the published version and must be bumped for each Maven Central release.
ThisBuild / version := "1.0.0"

// sbt-ci-release keys publishing off isSnapshot, and our compatibility tags are not
// Maven-semver, so snapshot status is derived from the source-controlled version above.
ThisBuild / isSnapshot := (ThisBuild / version).value.endsWith("-SNAPSHOT")
