# Changelog

All notable changes to this project will be documented in this file.

<!-- git-cliff-unreleased-start -->
## 0.1.5 - **not yet released**

### 🚀 Features

- Fake clock support for testing ([#41](https://github.com/apify/crawlee-storage/pull/41)) ([0b9a28f](https://github.com/apify/crawlee-storage/commit/0b9a28ffd3df21a92ade9a6e597a9aeac2e2a4bf)) by [@janbuchar](https://github.com/janbuchar)
- Return native Date from JS bindings ([#49](https://github.com/apify/crawlee-storage/pull/49)) ([6c66ebf](https://github.com/apify/crawlee-storage/commit/6c66ebf4623f622f4f96d76543fa9524afbcbce9)) by [@janbuchar](https://github.com/janbuchar)
- Add `prefix` option to KVS listKeys, flip assumeSoleOwner default ([#56](https://github.com/apify/crawlee-storage/pull/56)) ([01f8157](https://github.com/apify/crawlee-storage/commit/01f81576f30c14e2d151606e9fef315b5a3401c4)) by [@janbuchar](https://github.com/janbuchar)
- [**breaking**] Replace require_record_metadata flag with a higher-level resolveValue method ([#60](https://github.com/apify/crawlee-storage/pull/60)) ([0930389](https://github.com/apify/crawlee-storage/commit/09303899f4d8e29c65393619613286add934864a)) by [@janbuchar](https://github.com/janbuchar)
- GetPublicUrl checks for existence, listKeys validates exclusiveStartKey ([#61](https://github.com/apify/crawlee-storage/pull/61)) ([4e6d32f](https://github.com/apify/crawlee-storage/commit/4e6d32f7038846943f0d737d06fbcaa49521a245)) by [@janbuchar](https://github.com/janbuchar)
- Add option to include selected bare files in listKeys output ([#69](https://github.com/apify/crawlee-storage/pull/69)) ([b64a6ba](https://github.com/apify/crawlee-storage/commit/b64a6bacab03b945c97ff5f5af8dcc200d191000)) by [@janbuchar](https://github.com/janbuchar)

### 🐛 Bug Fixes

- Prevent concurrent open() calls from clobbering request queue locks ([#42](https://github.com/apify/crawlee-storage/pull/42)) ([3de01ee](https://github.com/apify/crawlee-storage/commit/3de01ee434b0fd4687c0e6ff7787d0ed4d50849f)) by [@janbuchar](https://github.com/janbuchar)
- Fix re-insertion of handled requests ([#43](https://github.com/apify/crawlee-storage/pull/43)) ([e9ad141](https://github.com/apify/crawlee-storage/commit/e9ad141f09193b0c7edd591031139fd238d077c6)) by [@janbuchar](https://github.com/janbuchar)
- Improve Python types ([#50](https://github.com/apify/crawlee-storage/pull/50)) ([339f765](https://github.com/apify/crawlee-storage/commit/339f765752d39f34ee51255c98099503be8d269b)) by [@janbuchar](https://github.com/janbuchar)
- Fix node tests and run more checks in CI ([#51](https://github.com/apify/crawlee-storage/pull/51)) ([5e32037](https://github.com/apify/crawlee-storage/commit/5e320370e38deb38c4fb8da9bf4e504d86911232)) by [@janbuchar](https://github.com/janbuchar)
- **ci:** Fix postbuild npm script ([#52](https://github.com/apify/crawlee-storage/pull/52)) ([46447b7](https://github.com/apify/crawlee-storage/commit/46447b752aa139514cf6fcf254def1932a7d1dc7)) by [@janbuchar](https://github.com/janbuchar)
- Reduce the amount of nullable fields in the public API ([#53](https://github.com/apify/crawlee-storage/pull/53)) ([96742e1](https://github.com/apify/crawlee-storage/commit/96742e1e627cb1bfca213728abf74d70cb33d81d)) by [@janbuchar](https://github.com/janbuchar)
- Improve handling of bare KeyValueStore records ([#58](https://github.com/apify/crawlee-storage/pull/58)) ([73be116](https://github.com/apify/crawlee-storage/commit/73be1160e1640754aa86a7afdb034aa41fb34344)) by [@janbuchar](https://github.com/janbuchar)
- Re-add accidentally deleted KeyValueStore.delete_value method in Python binding ([#62](https://github.com/apify/crawlee-storage/pull/62)) ([593ccb4](https://github.com/apify/crawlee-storage/commit/593ccb4a4c313ba10a91462b55facc0eff96d22a)) by [@janbuchar](https://github.com/janbuchar)

### 🚜 Refactor

- [**breaking**] Assume_sole_owner flag -&gt; request_queue_access enum ([#68](https://github.com/apify/crawlee-storage/pull/68)) ([5b03441](https://github.com/apify/crawlee-storage/commit/5b03441a7f1aa7d34e6cd01e675b58c40bf15c0a)) by [@janbuchar](https://github.com/janbuchar)


<!-- git-cliff-unreleased-end -->
# Changelog

All notable changes to this project will be documented in this file.