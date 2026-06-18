/* eslint-disable no-console */
// Adds a `-beta.N` suffix to the npm package version before a prerelease publish.
//
// This is a faithful-ish port of apify-client-js's before-beta-release.js. It exists
// because the upstream `git-cliff-release` action emits bare versions (e.g. 0.1.5)
// even for `release_type: prerelease`, so we tack on the suffix at publish time.
// The committed package.json keeps the bare version; only the working copy is
// rewritten right before `npm publish`.
//
// TODO: This is a temporary hack. The proper fix is upstream in apify/actions
// (or apify/workflows) git-cliff-release, ideally discovering existing prerelease
// numbers from Git tags so it works for npm, PyPI, and crates.io alike. When that
// lands across all relevant repos, delete this script.
const fs = require('node:fs');
const path = require('node:path');

const PKG_JSON_PATH = path.join(__dirname, '..', '..', 'crawlee-storage-node', 'package.json');

const pkgJson = JSON.parse(fs.readFileSync(PKG_JSON_PATH, 'utf8'));

const PACKAGE_NAME = pkgJson.name;
const VERSION = pkgJson.version;

main().catch((err) => {
    console.error(err);
    process.exit(1);
});

async function main() {
    const nextVersion = await addBetaSuffixToVersion(VERSION);
    console.log(`before-deploy: Setting version to ${nextVersion}`);
    pkgJson.version = nextVersion;
    fs.writeFileSync(PKG_JSON_PATH, `${JSON.stringify(pkgJson, null, 4)}\n`);
}

async function addBetaSuffixToVersion(version) {
    // Fetch the full packument from the registry so we can see per-version
    // `deprecated` markers. `npm show <pkg> versions --json` does not include
    // them, and `npm show <pkg> deprecated --json` only returns the latest
    // version's message. Deprecated versions are tombstoned and don't count
    // toward the collision check.
    const url = `https://registry.npmjs.org/${encodeURIComponent(PACKAGE_NAME).replace('%40', '@')}`;
    const res = await fetch(url);
    if (!res.ok) {
        throw new Error(`Failed to fetch packument for ${PACKAGE_NAME}: ${res.status} ${res.statusText}`);
    }
    const packument = await res.json();
    const versions = Object.entries(packument.versions ?? {})
        .filter(([, info]) => !info.deprecated)
        .map(([v]) => v);

    if (versions.some((v) => v === version)) {
        console.error(
            `before-deploy: A release with version ${version} already exists. Please increment version accordingly.`,
        );
        process.exit(1);
    }

    const prereleaseNumbers = versions
        .filter((v) => v.startsWith(`${version}-`))
        .map((v) => Number(v.match(/\.(\d+)$/)?.[1]))
        .filter((n) => Number.isFinite(n));
    const lastPrereleaseNumber = Math.max(-1, ...prereleaseNumbers);
    return `${version}-beta.${lastPrereleaseNumber + 1}`;
}
