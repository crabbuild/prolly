#!/usr/bin/env node

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const manifestPath = resolve(process.argv[2] ?? join(root, "conformance/store-protocol-v1/compatibility.json"));
const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));

const sharedProviders = ["sqlite", "postgresql", "mysql", "redis", "dynamodb", "cosmosdb", "spanner"];
const nodeProviders = [...sharedProviders, "pglite"];
const expectedSdks = {
  node: {
    sqlite: ["better-sqlite3", "12.11.1"],
    postgresql: ["pg", "8.22.0"],
    mysql: ["mysql2", "3.23.0"],
    redis: ["redis", "6.1.0"],
    dynamodb: ["@aws-sdk/client-dynamodb", "3.1089.0"],
    cosmosdb: ["@azure/cosmos", "4.9.3"],
    spanner: ["@google-cloud/spanner", "8.0.0"],
    pglite: ["@electric-sql/pglite", "0.5.4"],
  },
  kotlin: {
    sqlite: ["org.xerial:sqlite-jdbc", "3.53.2.0"],
    postgresql: ["org.postgresql:postgresql", "42.7.13"],
    mysql: ["com.mysql:mysql-connector-j", "9.7.0"],
    redis: ["io.lettuce:lettuce-core", "7.6.0.RELEASE"],
    dynamodb: ["software.amazon.awssdk:dynamodb", "2.48.2"],
    cosmosdb: ["com.azure:azure-cosmos", "4.81.0"],
    spanner: ["com.google.cloud:google-cloud-spanner", "6.119.0"],
  },
};
expectedSdks.java = expectedSdks.kotlin;

const capabilityKeys = [
  "native_batch_reads", "atomic_batch_writes", "node_scan", "hints",
  "atomic_nodes_and_hint", "root_scan", "root_compare_and_swap", "transactions",
];
const limitKeys = [
  "max_batch_read_items", "max_batch_write_items", "max_transaction_operations", "max_node_bytes",
];

function fail(message) {
  throw new Error(`store compatibility verification failed: ${message}`);
}

function requireSupported(language, providers) {
  const cells = manifest.languages?.[language];
  if (!cells) fail(`missing language ${language}`);
  for (const provider of providers) {
    const cell = cells[provider];
    if (!cell) fail(`missing ${language}/${provider}`);
    if (cell.status !== "supported") fail(`${language}/${provider} must be supported`);
  }
}

function verifyCell(language, provider, cell) {
  if (cell.status === "unsupported") {
    if (typeof cell.reason !== "string" || cell.reason.trim().length < 20 || /todo|tbd|placeholder/i.test(cell.reason)) {
      fail(`${language}/${provider} needs a concrete unsupported reason`);
    }
    const supportedOnly = ["module", "sdk_module", "sdk_version", "capabilities", "limits", "evidence"];
    for (const key of supportedOnly) {
      if (key in cell) fail(`${language}/${provider} unsupported entry must not claim ${key}`);
    }
    return;
  }
  if (cell.status !== "supported") fail(`${language}/${provider} has invalid status ${String(cell.status)}`);
  for (const key of ["module", "sdk_module", "sdk_version"]) {
    if (typeof cell[key] !== "string" || cell[key].length === 0) fail(`${language}/${provider} missing ${key}`);
  }
  if (cell.protocol_major !== manifest.protocol_major || cell.schema_version !== 1) {
    fail(`${language}/${provider} must declare protocol ${manifest.protocol_major} and schema 1`);
  }
  if (!cell.capabilities || !Number.isInteger(cell.capabilities.read_parallelism) || cell.capabilities.read_parallelism < 1) {
    fail(`${language}/${provider} must declare a positive integer read_parallelism`);
  }
  for (const key of capabilityKeys) {
    if (typeof cell.capabilities[key] !== "boolean") fail(`${language}/${provider} missing boolean capability ${key}`);
  }
  for (const key of limitKeys) {
    if (!(key in (cell.limits ?? {})) || (cell.limits[key] !== null && !Number.isInteger(cell.limits[key]))) {
      fail(`${language}/${provider} has invalid limit ${key}`);
    }
  }
  if (!Array.isArray(cell.evidence) || !cell.evidence.some((item) => typeof item === "string" && !item.startsWith("live:"))) {
    fail(`${language}/${provider} needs executable evidence`);
  }
}

assert.equal(manifest.protocol_major, 1, "only store protocol major 1 is accepted");
requireSupported("node", nodeProviders);
requireSupported("kotlin", sharedProviders);
requireSupported("java", sharedProviders);
requireSupported("go", sharedProviders);

for (const [language, providers] of Object.entries(manifest.languages ?? {})) {
  for (const [provider, cell] of Object.entries(providers)) verifyCell(language, provider, cell);
}

for (const [language, providers] of Object.entries(expectedSdks)) {
  for (const [provider, [sdk, version]] of Object.entries(providers)) {
    const cell = manifest.languages[language][provider];
    if (cell.sdk_module !== sdk || cell.sdk_version !== version) {
      fail(`${language}/${provider} SDK must be ${sdk}@${version}`);
    }
  }
}

const nodeDirectories = { postgresql: "postgres" };
for (const provider of nodeProviders) {
  const directory = nodeDirectories[provider] ?? provider;
  const packageJson = JSON.parse(readFileSync(join(root, `bindings/node/stores/${directory}/package.json`), "utf8"));
  const cell = manifest.languages.node[provider];
  if (packageJson.name !== cell.module || packageJson.dependencies?.[cell.sdk_module] !== cell.sdk_version) {
    fail(`Node package metadata disagrees with node/${provider}`);
  }
}

for (const language of ["kotlin", "java"]) {
  for (const provider of sharedProviders) {
    const directory = provider === "postgresql" ? "postgres" : provider;
    const pom = readFileSync(join(root, `bindings/${language}/stores/${directory}/pom.xml`), "utf8");
    const artifact = manifest.languages[language][provider].module.split(":")[1];
    if (!pom.includes(`<artifactId>${artifact}</artifactId>`)) fail(`${language}/${provider} POM has the wrong artifact`);
    if (language === "kotlin") {
      const [group, sdkArtifact] = expectedSdks.kotlin[provider][0].split(":");
      const version = expectedSdks.kotlin[provider][1];
      if (!pom.includes(`<groupId>${group}</groupId>`) || !pom.includes(`<artifactId>${sdkArtifact}</artifactId>`) || !pom.includes(`<version>${version}</version>`)) {
        fail(`Kotlin ${provider} POM does not pin ${group}:${sdkArtifact}:${version}`);
      }
    }
  }
}

const forbiddenCoreDependencies = [
  "better-sqlite3", "@electric-sql/pglite", "@aws-sdk/client-dynamodb", "@azure/cosmos", "@google-cloud/spanner",
  "<artifactId>sqlite-jdbc</artifactId>", "<artifactId>postgresql</artifactId>", "<artifactId>mysql-connector-j</artifactId>",
  "<artifactId>lettuce-core</artifactId>", "<artifactId>dynamodb</artifactId>", "<artifactId>azure-cosmos</artifactId>",
  "<artifactId>google-cloud-spanner</artifactId>",
];
const coreFiles = ["bindings/node/package.json", "bindings/kotlin/pom.xml", "bindings/java/pom.xml"];
for (const path of coreFiles) {
  const content = readFileSync(join(root, path), "utf8");
  for (const dependency of forbiddenCoreDependencies) {
    if (content.includes(dependency)) fail(`${path} contains provider dependency ${dependency}`);
  }
}

console.log(`store compatibility manifest valid: ${nodeProviders.length} Node, ${sharedProviders.length} Kotlin, ${sharedProviders.length} Java, and ${sharedProviders.length} Go providers`);
