/* eslint-disable no-console */
// Adds a `-beta.N` suffix to the npm package version before a prerelease publish.
//
// This is a faithful port of apify-client-js's before-beta-release.js. It exists
// because the upstream `git-cliff-release` action emits bare versions (e.g. 0.1.5)
// even for `release_type: prerelease`, so we tack on the suffix at publish time.
// The committed package.json keeps the bare version; only the working copy is
// rewritten right before `npm publish`.
//
// TODO: This is a temporary hack. The proper fix is upstream in apify/actions
// (or apify/workflows) git-cliff-release, ideally discovering existing prerelease
// numbers from Git tags so it works for npm, PyPI, and crates.io alike. When that
// lands across all relevant repos, delete this script.
const { execSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

const PKG_JSON_PATH = path.join(__dirname, '..', '..', 'crawlee-storage-node', 'package.json');

const pkgJson = JSON.parse(fs.readFileSync(PKG_JSON_PATH, 'utf8'));

const PACKAGE_NAME = pkgJson.name;
const VERSION = pkgJson.version;

const nextVersion = addBetaSuffixToVersion(VERSION);
console.log(`before-deploy: Setting version to ${nextVersion}`);
pkgJson.version = nextVersion;

fs.writeFileSync(PKG_JSON_PATH, `${JSON.stringify(pkgJson, null, 4)}\n`);

function addBetaSuffixToVersion(version) {
    const versionString = execSync(`npm show ${PACKAGE_NAME} versions --json`, { encoding: 'utf8' });
    const versions = JSON.parse(versionString);

    if (versions.some((v) => v === version)) {
        console.error(
            `before-deploy: A release with version ${version} already exists. Please increment version accordingly.`,
        );
        process.exit(1);
    }

    const prereleaseNumbers = versions
        .filter((v) => v.startsWith(`${version}-`))
        .map((v) => Number(v.match(/\.(\d+)$/)[1]))
        .filter((n) => Number.isFinite(n));
    const lastPrereleaseNumber = Math.max(-1, ...prereleaseNumbers);
    return `${version}-beta.${lastPrereleaseNumber + 1}`;
}
