## [0.2.20](https://github.com/brillout/telefunc/compare/v0.2.19...v0.2.20) (2026-04-10)


### Bug Fixes

* @brillout/vite-plugin-server-entry@^0.7.18 ([#238](https://github.com/brillout/telefunc/issues/238)) ([7610d4c](https://github.com/brillout/telefunc/commit/7610d4cb06575fe78ae1a61e8855ecc08a6829ff))
* detect referenced tsconfig paths when locating function tsconfig files ([#241](https://github.com/brillout/telefunc/issues/241)) ([bf88d97](https://github.com/brillout/telefunc/commit/bf88d97b189a0c24356ad1712b6fda3a03ba82d1))



## [0.2.19](https://github.com/brillout/telefunc/compare/v0.2.18...v0.2.19) (2026-02-22)


### Bug Fixes

* @brillout/json-serializer@^0.5.22 ([604a8cf](https://github.com/brillout/telefunc/commit/604a8cfc25917e65f536a9be1378fccf06795567))
* avoid TypeScript warning on unused generated shield type (fix [#229](https://github.com/brillout/telefunc/issues/229)) ([#231](https://github.com/brillout/telefunc/issues/231)) ([6f0396d](https://github.com/brillout/telefunc/commit/6f0396da8e07d5f742ef2fc59d11af655779d423))


### Features

* file uploads ([#234](https://github.com/brillout/telefunc/issues/234)) ([cb80f02](https://github.com/brillout/telefunc/commit/cb80f0248c194ad111eea0c9a761f6d4f29586f5))



## [0.2.18](https://github.com/brillout/telefunc/compare/v0.2.17...v0.2.18) (2026-02-13)


### Bug Fixes

* update glob pattern to work with Vite 8 ([a6c047b](https://github.com/brillout/telefunc/commit/a6c047bb527a5d1287f47e7fc2f795a97fae51d0))



## [0.2.17](https://github.com/brillout/telefunc/compare/v0.2.16...v0.2.17) (2025-11-13)


### Bug Fixes

* remove 'telefunc' from optimizeDeps.include ([#223](https://github.com/brillout/telefunc/issues/223)) ([8075bda](https://github.com/brillout/telefunc/commit/8075bda0b9cb0dc85cad2d10e3e77d9edeea1e0c))
* revert workaround Vite bug upon import picocolors in utils/assert.ts ([32e11e3](https://github.com/brillout/telefunc/commit/32e11e36f4b66c647ddfac11afb8b6b92a9486d2))



## [0.2.16](https://github.com/brillout/telefunc/compare/v0.2.15...v0.2.16) (2025-11-10)


### Bug Fixes

* workaround Vite bug upon import picocolors in utils/assert.ts ([d9967bd](https://github.com/brillout/telefunc/commit/d9967bd1541d6f6eef7ffdf793ce9bff5e4a16c3))



## [0.2.15](https://github.com/brillout/telefunc/compare/v0.2.14...v0.2.15) (2025-10-13)


### Bug Fixes

* improve shield() validation error logging ([#57](https://github.com/brillout/telefunc/issues/57)) ([c8a657e](https://github.com/brillout/telefunc/commit/c8a657e9bee0cd3701cabd0bfc0e5a1e632e416e))
* show `422` instead of `403` upon `shield()` validation error (fix [#57](https://github.com/brillout/telefunc/issues/57)) ([#216](https://github.com/brillout/telefunc/issues/216)) ([c3c2f15](https://github.com/brillout/telefunc/commit/c3c2f15ba16471f17d05556a6b3dc9bd058bcb85))
* show shield() validation errors in production by default ([#57](https://github.com/brillout/telefunc/issues/57)) ([d7e0873](https://github.com/brillout/telefunc/commit/d7e087304a1cd1ed337052a8106cf9cbe707b59f))



## [0.2.14](https://github.com/brillout/telefunc/compare/v0.2.13...v0.2.14) (2025-10-06)


### Bug Fixes

* colorize message prefix ([e6a91d0](https://github.com/brillout/telefunc/commit/e6a91d0c836cc470b863685b362bb96560d7d692))
* don't add generated shield code to ts-morph project ([#215](https://github.com/brillout/telefunc/issues/215)) ([8feb6e9](https://github.com/brillout/telefunc/commit/8feb6e9c0be623d0dc83f838e28d33737273babb))
* improve TypeScript error diagnostic upon shield() generation failure ([#215](https://github.com/brillout/telefunc/issues/215)) ([7154916](https://github.com/brillout/telefunc/commit/7154916f1ad0e994293b5858b82ad256615920f0))
* tolerate non-function exports at build-time ([#215](https://github.com/brillout/telefunc/issues/215)) ([6c6104f](https://github.com/brillout/telefunc/commit/6c6104f040379065a538efbb53db548cf3f2cd54))
* tolerate non-function exports but show a warning (fix [#215](https://github.com/brillout/telefunc/issues/215)) ([48de2a0](https://github.com/brillout/telefunc/commit/48de2a0d9010b781ab450fbd147a35fd7b154aee))



## [0.2.13](https://github.com/brillout/telefunc/compare/v0.2.12...v0.2.13) (2025-09-11)


### Bug Fixes

* support @cloudflare/vite-plugin ([#209](https://github.com/brillout/telefunc/issues/209)) ([b15355e](https://github.com/brillout/telefunc/commit/b15355ee8090bc5c6d9ebc945dc33f6079d82005))



## [0.2.12](https://github.com/brillout/telefunc/compare/v0.2.11...v0.2.12) (2025-09-03)


### Bug Fixes

* update @brillout/vite-plugin-server-entry ([91721f4](https://github.com/brillout/telefunc/commit/91721f45c3233dd1d3b261c07c4c2b46a3d8d7bb))


### Performance Improvements

* use Vite hook filters ([#208](https://github.com/brillout/telefunc/issues/208)) ([28c6498](https://github.com/brillout/telefunc/commit/28c649808e6046440bb5a24bbffdda6194440f51))


### MINOR BREAKING CHANGES

> [!NOTE]
> We recommend ignoring `MINOR BREAKING CHANGES` unless this version breaks your app, see [Vike Versioning](https://vike.dev/versioning).

* Update to Vite `6.3.0` or above



## [0.2.11](https://github.com/brillout/telefunc/compare/v0.2.10...v0.2.11) (2025-08-25)


### Bug Fixes

* update @brillout/vite-plugin-server-entry ([#206](https://github.com/brillout/telefunc/issues/206)) ([bbd006a](https://github.com/brillout/telefunc/commit/bbd006ae6513286c03108086ca4f081c4d6f9894))



## [0.2.10](https://github.com/brillout/telefunc/compare/v0.2.9...v0.2.10) (2025-08-21)


### Bug Fixes

* @brillout/json-serializer@^0.5.19 ([49fe95f](https://github.com/brillout/telefunc/commit/49fe95f0254454346bd5e1c56a69a859b3c2a95b))
* @brillout/json-serializer@^0.5.20 ([fe52288](https://github.com/brillout/telefunc/commit/fe522881d4083bca56b76c3c608d0da9222a7f1e))
* use virtual file instead of physical file ([#204](https://github.com/brillout/telefunc/issues/204)) ([a9e8807](https://github.com/brillout/telefunc/commit/a9e880738bc4cec88e6ff4e4898b0811ff23046b))


### MINOR BREAKING CHANGES

> [!NOTE]
> We recommend ignoring `MINOR BREAKING CHANGES` unless this version breaks your app, see [Vike Versioning](https://vike.dev/versioning).

* Update to Node.js 20 or above (or use Bun/Deno). See also: [`require(esm)`](https://gist.github.com/brillout/8e0133716e169b981b6c4e8a938b0134).



## [0.2.9](https://github.com/brillout/telefunc/compare/v0.2.8...v0.2.9) (2025-08-20)


### Bug Fixes

* avoid Cloudflare false warning about Node.js API usage ([66f2a5f](https://github.com/brillout/telefunc/commit/66f2a5f186f5e46b5ca25efb36e8a7f09f00451d))
* avoid loading modules only needed for Vite in production ([#199](https://github.com/brillout/telefunc/issues/199)) ([10ee58d](https://github.com/brillout/telefunc/commit/10ee58d99cf15a174a93261f1e591e49b55869c5))
* improve browser test ([a234369](https://github.com/brillout/telefunc/commit/a2343695f9eb7e0f456cab76b5a5e1c83aed7a0b))
* improve Vite server/client check ([#202](https://github.com/brillout/telefunc/issues/202)) ([37762cd](https://github.com/brillout/telefunc/commit/37762cd76ebab4722fbed9f875100d383076ef12))
* let frameworks handle build.outDir ([#201](https://github.com/brillout/telefunc/issues/201)) ([f0fd1fc](https://github.com/brillout/telefunc/commit/f0fd1fc845d3bc4068ce50ffb752c8f5005782ac))
* require Vite 6 ([#198](https://github.com/brillout/telefunc/issues/198)) ([f2eeb75](https://github.com/brillout/telefunc/commit/f2eeb75a08bbf1c59b1a6ab580cd067804ae6ea0))
* update @brillout/vite-plugin-server-entry ([#193](https://github.com/brillout/telefunc/issues/193)) ([152c30b](https://github.com/brillout/telefunc/commit/152c30bcbafcca6330961a11287c2f724c035ada))


### MINOR BREAKING CHANGES

> [!NOTE]
> We recommend ignoring `MINOR BREAKING CHANGES` unless this version breaks your app, see [Vike Versioning](https://vike.dev/versioning).

* If you use Vite, then update to Vite 6 or above.



## [0.2.8](https://github.com/brillout/telefunc/compare/v0.2.7...v0.2.8) (2025-06-25)


### Bug Fixes

* update ts-morph (fix [#181](https://github.com/brillout/telefunc/issues/181)) ([796c97f](https://github.com/brillout/telefunc/commit/796c97f42ce686b58724b59a3ba874cb037daf07))



## [0.2.7](https://github.com/brillout/telefunc/compare/v0.2.6...v0.2.7) (2025-05-22)


### Bug Fixes

* use registration method as fallback (brillout/vite-plugin-server-entry[#21](https://github.com/brillout/telefunc/issues/21)) ([#172](https://github.com/brillout/telefunc/issues/172)) ([1bb7aea](https://github.com/brillout/telefunc/commit/1bb7aea46112b40cdea69f795f11ccb306ae05eb))



## [0.2.6](https://github.com/brillout/telefunc/compare/v0.2.5...v0.2.6) (2025-05-15)


### Features

* add custom fetch option in client side (closes [#60](https://github.com/brillout/telefunc/issues/60)) ([#168](https://github.com/brillout/telefunc/issues/168)) ([e1443b5](https://github.com/brillout/telefunc/commit/e1443b50f63949863f78e1b9f24408b3a8bcfc37))



## [0.2.5](https://github.com/brillout/telefunc/compare/v0.2.4...v0.2.5) (2025-04-23)


### Bug Fixes

* fix HMR + shield.dev (fix [#165](https://github.com/brillout/telefunc/issues/165)) ([deaffcc](https://github.com/brillout/telefunc/commit/deaffccb8e4fd8290325c0f87c109323d6ef4b23))



## [0.2.4](https://github.com/brillout/telefunc/compare/v0.2.3...v0.2.4) (2025-04-22)


### Bug Fixes

* @brillout/vite-plugin-server-entry@^0.7.6 ([92bc8a2](https://github.com/brillout/telefunc/commit/92bc8a224632d4f2f954b662472c0b99d34cb164))


### Features

* add option for turning off shield in prod (closes [#116](https://github.com/brillout/telefunc/issues/116)) ([#164](https://github.com/brillout/telefunc/issues/164)) ([383dbe1](https://github.com/brillout/telefunc/commit/383dbe1cd2147f6f7fa4995ab4de4c16d7b8b40d))



## [0.2.3](https://github.com/brillout/telefunc/compare/v0.2.2...v0.2.3) (2025-04-04)


### Bug Fixes

* fix source map ([#161](https://github.com/brillout/telefunc/issues/161)) ([8452eb0](https://github.com/brillout/telefunc/commit/8452eb0fb858af869203c07f8ed53ec99ea9351f))



## [0.2.2](https://github.com/brillout/telefunc/compare/v0.2.1...v0.2.2) (2025-03-21)


### Bug Fixes

* update @brillout/vite-plugin-server-entry@^0.7.1 ([#156](https://github.com/brillout/telefunc/issues/156)) ([d0464bd](https://github.com/brillout/telefunc/commit/d0464bd8e9fa11838d23f4ad4775d905cf0722de))



## [0.2.1](https://github.com/brillout/telefunc/compare/v0.2.0...v0.2.1) (2025-03-13)


### Bug Fixes

* avoid NextConfig version mismatch ([f9ff014](https://github.com/brillout/telefunc/commit/f9ff014f6f78a44ccd240c4dffdef184016f85a1))



# [0.2.0](https://github.com/brillout/telefunc/compare/v0.1.87...v0.2.0) (2025-03-13)


### Bug Fixes

* improve client-side error upon wrong usage ([#152](https://github.com/brillout/telefunc/issues/152)) ([487969a](https://github.com/brillout/telefunc/commit/487969af8486ae206fa7a996472a27db37b3c911))
* publish as ESM only ([#154](https://github.com/brillout/telefunc/issues/154)) ([4ace609](https://github.com/brillout/telefunc/commit/4ace60983c8989beeb904c99c62ff979ed92f8e4))


### MINOR BREAKING CHANGES

> [!NOTE]
> We recommend ignoring `MINOR BREAKING CHANGES` unless this version breaks your app, see [Vike Versioning](https://vike.dev/versioning).

* If your app is CJS then update Node.js to v23 or above (or v22 with the `--experimental-require-module` flag) in order to [be able to `require(esm)`](https://nodejs.org/en/blog/announcements/v22-release-announce#support-requireing-synchronous-esm-graphs) . If your app is ESM then you can keep using older Node.js versions.



## [0.1.87](https://github.com/brillout/telefunc/compare/v0.1.86...v0.1.87) (2025-03-06)


### Features

* [`config.log.shieldErrors`](http://telefunc.com/log) (closes [#149](https://github.com/brillout/telefunc/issues/149)) ([#150](https://github.com/brillout/telefunc/issues/150)) ([f7ef230](https://github.com/brillout/telefunc/commit/f7ef2306f78ab555c0c3e72afa32ee7068c9a1cf))



## [0.1.86](https://github.com/brillout/telefunc/compare/v0.1.85...v0.1.86) (2025-02-27)


### Bug Fixes

* optimistic peer dep semver (fix [#147](https://github.com/brillout/telefunc/issues/147)) ([38080ee](https://github.com/brillout/telefunc/commit/38080ee3c1f2f15964f2f1547f431436fa386411))
* update @brillout/import ([078bd1e](https://github.com/brillout/telefunc/commit/078bd1e42df3debc6c75d423bf2de967dd08ee55))
* update @brillout/picocolors ([0d71799](https://github.com/brillout/telefunc/commit/0d71799d862ca57cfbcd23a8bc05250b5745a8b7))
* update @brillout/vite-plugin-server-entry ([ba1891f](https://github.com/brillout/telefunc/commit/ba1891f9e632c3205e0be561ab5590fa8648b79e))
* update @brillout/vite-plugin-server-entry ([8cd2203](https://github.com/brillout/telefunc/commit/8cd22033ba5558be82d2c547755e3ce9c3bcaad3))



## [0.1.85](https://github.com/brillout/telefunc/compare/v0.1.84...v0.1.85) (2025-01-31)


### Bug Fixes

* apply shield() to all exports ([#141](https://github.com/brillout/telefunc/issues/141)) ([04a287a](https://github.com/brillout/telefunc/commit/04a287a2be55380eaaf7892d14b74bd670085edc))
* colorize error message ([20ec17b](https://github.com/brillout/telefunc/commit/20ec17b31ce6795e23fee793901f4d2454cd85d4))
* support export renaming (fix [#138](https://github.com/brillout/telefunc/issues/138)) ([#139](https://github.com/brillout/telefunc/issues/139)) ([9dc86a4](https://github.com/brillout/telefunc/commit/9dc86a4fef29b180b60dc6996ac70e207027e3f5))
* update es-module-lexer ([3b9944e](https://github.com/brillout/telefunc/commit/3b9944e5e9ad4514cff028524e40404b9113babe))



## [0.1.84](https://github.com/brillout/telefunc/compare/v0.1.83...v0.1.84) (2025-01-31)


### Bug Fixes

* simplify path handling + remove Node.js dependency (fix [#137](https://github.com/brillout/telefunc/issues/137)) ([ddb5796](https://github.com/brillout/telefunc/commit/ddb5796eda7e6824ec1d9bce5bdf4b0bcbb39d57))



## [0.1.83](https://github.com/brillout/telefunc/compare/v0.1.82...v0.1.83) (2025-01-09)


### Bug Fixes

* support arrow functions (fix [#134](https://github.com/brillout/telefunc/issues/134)) ([da662ef](https://github.com/brillout/telefunc/commit/da662ef9023ccf49af7c2cd15ff9d0217924b3f7))
* update error message ([981b6a2](https://github.com/brillout/telefunc/commit/981b6a2d5406ce5d797d2ecf014a11a697d799b1))



## [0.1.82](https://github.com/brillout/telefunc/compare/v0.1.81...v0.1.82) (2024-12-03)


### Bug Fixes

* [revert] make default condition last again ([28d4275](https://github.com/brillout/telefunc/commit/28d4275ecabdc6a24afd5d7395ce6c311b365dbf))
* add node condition ([91ff12e](https://github.com/brillout/telefunc/commit/91ff12ec75dcf0a76000ca3b4e359f65e73e7629))
* add poisen pills ([dbc83a3](https://github.com/brillout/telefunc/commit/dbc83a3054c5a8e63140c5a4d6278cdf20648e68))
* add worker condition ([85a8d4d](https://github.com/brillout/telefunc/commit/85a8d4dea89bcc90bfd5aabc997ba23ded6f2f18))
* ensure default export works ([8e4e61d](https://github.com/brillout/telefunc/commit/8e4e61d3f0690102a9d9ea0e6ece0c6cd472d0a1))
* improve exports mapping ([b58bb3f](https://github.com/brillout/telefunc/commit/b58bb3f2d790d128924774a7cfd5003ebee42d48))
* make condition `types` last ([7d21ebf](https://github.com/brillout/telefunc/commit/7d21ebf2b787c7c15d9582b2e4f6bb1a1c9911f6))
* use cjs build only for node condition ([8aa29e4](https://github.com/brillout/telefunc/commit/8aa29e451b394c16cbb29799d3701ebc6a818317))
* use setTimeout() instead of process.nextTick() in runtime ([#117](https://github.com/brillout/telefunc/issues/117)) ([0fd4432](https://github.com/brillout/telefunc/commit/0fd44322acbd07857ae29361ba7c998607f17dd5))
* use shim instead of node:path in runtime code ([#117](https://github.com/brillout/telefunc/issues/117)) ([#126](https://github.com/brillout/telefunc/issues/126)) ([6a47ec2](https://github.com/brillout/telefunc/commit/6a47ec25e9234fa8ce07026feaf0e5f1d2722aaa))



## [0.1.81](https://github.com/brillout/telefunc/compare/v0.1.80...v0.1.81) (2024-11-02)


### Bug Fixes

* eagerly try Vite telefunc file retrieval (fix [#121](https://github.com/brillout/telefunc/issues/121)) ([44a6653](https://github.com/brillout/telefunc/commit/44a6653074d989c32d64e3f0e5fc0cda1864cedb))
* improve error message ([3445e13](https://github.com/brillout/telefunc/commit/3445e13e6535717bf8b3ffd493924fb7e9593ece))
* update picocolors ([453b43c](https://github.com/brillout/telefunc/commit/453b43c93af6bdf021f0a40a1d6d3c89115615c8))



## [0.1.80](https://github.com/brillout/telefunc/compare/v0.1.79...v0.1.80) (2024-10-28)


### Bug Fixes

* update typescript runtime validation to work with newer TypeScript versions ([#125](https://github.com/brillout/telefunc/issues/125)) ([5c3799b](https://github.com/brillout/telefunc/commit/5c3799b121ad7e0df077a82a5983ed741b46ed68))



## [0.1.79](https://github.com/brillout/telefunc/compare/v0.1.78...v0.1.79) (2024-09-07)


### Bug Fixes

* improve isWebpack() test (fix [#121](https://github.com/brillout/telefunc/issues/121)) ([1e6403e](https://github.com/brillout/telefunc/commit/1e6403e4b8437d3da49bc3bbab5747749f0a7e68))



## [0.1.78](https://github.com/brillout/telefunc/compare/v0.1.77...v0.1.78) (2024-08-25)


### Bug Fixes

* remove picocolors dependency ([5295795](https://github.com/brillout/telefunc/commit/529579587d5f9ad8aee633643709fb3ad2516941))
* update @brillout/vite-plugin-server-entry ([#118](https://github.com/brillout/telefunc/issues/118)) (fix vikejs/vike[#1827](https://github.com/brillout/telefunc/issues/1827)) ([cfa76c6](https://github.com/brillout/telefunc/commit/cfa76c6cf9b17b4b6116f8d29a62372a8077bf1e))
* update es-module-lexer ([4170fad](https://github.com/brillout/telefunc/commit/4170fadf90d412dc8ae15a80d81c910a597bbaec))



## [0.1.77](https://github.com/brillout/telefunc/compare/v0.1.76...v0.1.77) (2024-08-23)


### Bug Fixes

* update @brillout/vite-plugin-server-entry ([0c4a043](https://github.com/brillout/telefunc/commit/0c4a04389a316f32bf0499bc8f2eb7dadc49f999))



## [0.1.76](https://github.com/brillout/telefunc/compare/v0.1.75...v0.1.76) (2024-07-02)


### Bug Fixes

* add `node:` prefix to Node.js imports (fix [#113](https://github.com/brillout/telefunc/issues/113)) ([#115](https://github.com/brillout/telefunc/issues/115)) ([f9a3015](https://github.com/brillout/telefunc/commit/f9a3015bf8a63515d02408a765fd110b52359b43))



## [0.1.75](https://github.com/brillout/telefunc/compare/v0.1.74...v0.1.75) (2024-06-28)


### Features

* `config.shield.dev` ([#111](https://github.com/brillout/telefunc/issues/111)) ([2830461](https://github.com/brillout/telefunc/commit/283046149129b99509ab426c9cb42a007c2bb6ac))



## [0.1.74](https://github.com/brillout/telefunc/compare/v0.1.73...v0.1.74) (2024-06-04)


### Bug Fixes

* close Discord channel ([2b065bd](https://github.com/brillout/telefunc/commit/2b065bda50038f5ee4ba00959490c04021c207b4))
* improve assert() error message ([11d2833](https://github.com/brillout/telefunc/commit/11d28330b96b1ced00a2a7439a727913c84bb89b))



## [0.1.73](https://github.com/brillout/telefunc/compare/v0.1.72...v0.1.73) (2024-05-27)


### Bug Fixes

* avoid @types/react version mismatch ([14008d7](https://github.com/brillout/telefunc/commit/14008d77c6ee6dd58c1c7221e466c4182328c8a4))



## [0.1.72](https://github.com/brillout/telefunc/compare/v0.1.71...v0.1.72) (2024-04-17)


### Bug Fixes

* improve error message upon wrong tsconfig.json configuration (fix [#100](https://github.com/brillout/telefunc/issues/100)) ([18ec0ce](https://github.com/brillout/telefunc/commit/18ec0ced9e09ae6fd5a2fa9e6806be0a1638e017))
* let user decide stack trace size ([5b299a8](https://github.com/brillout/telefunc/commit/5b299a86abb737589a116c4b907e2cd8e309d6ba))
* update @brillout/vite-plugin-server-entry ([4581bfa](https://github.com/brillout/telefunc/commit/4581bfa1842d19946508b293778c8323adaa87ab))



## [0.1.71](https://github.com/brillout/telefunc/compare/v0.1.70...v0.1.71) (2024-02-07)


### Bug Fixes

* improve error messages & docs for config.telefuncUrl (fix [#63](https://github.com/brillout/telefunc/issues/63)) ([1585b66](https://github.com/brillout/telefunc/commit/1585b66f0b8a399c3246118a232bf78094474973))
* soft-deprecate setting config over Vite plugin ([#63](https://github.com/brillout/telefunc/issues/63)) ([2657f86](https://github.com/brillout/telefunc/commit/2657f8601d1dbbd22c1e43014a61543da3d40974))



## [0.1.70](https://github.com/brillout/telefunc/compare/v0.1.69...v0.1.70) (2024-01-07)


### Bug Fixes

* update @brillout/vite-plugin-server-entry ([d17d6dc](https://github.com/brillout/telefunc/commit/d17d6dc7d0fed623c0f6caf960b46960d2b4ede1))



## [0.1.69](https://github.com/brillout/telefunc/compare/v0.1.68...v0.1.69) (2024-01-07)


### Bug Fixes

* update @brillout/vite-plugin-import-build -> @brillout/vite-plugin-server-entry ([8f9e77a](https://github.com/brillout/telefunc/commit/8f9e77a83acf5a694d427431136437cd37f2d0ce))



## [0.1.68](https://github.com/brillout/telefunc/compare/v0.1.67...v0.1.68) (2024-01-01)


### Bug Fixes

* further improve error messages (fix [#96](https://github.com/brillout/telefunc/issues/96)) ([1bc4d05](https://github.com/brillout/telefunc/commit/1bc4d05db10464412d88be829d4e19b7c2e2f74f))
* improve error message (fix [#94](https://github.com/brillout/telefunc/issues/94)) ([218326f](https://github.com/brillout/telefunc/commit/218326faa54d3ce6ac49f92b41dad36117169749))
* improve error message (fix [#95](https://github.com/brillout/telefunc/issues/95)) ([49e0ca3](https://github.com/brillout/telefunc/commit/49e0ca323ab9410e3330c763bcf6b94203a2b0b6))
* improve error message (fix [#96](https://github.com/brillout/telefunc/issues/96)) ([d42f963](https://github.com/brillout/telefunc/commit/d42f9630b1a337526ffc4742755e1792f6441b8e))



## [0.1.67](https://github.com/brillout/telefunc/compare/v0.1.66...v0.1.67) (2023-12-22)


### Bug Fixes

* serialize manifest instead of emitting it ([a085605](https://github.com/brillout/telefunc/commit/a085605b213b8062fe8f644e52aa9aaeb307e95e))
* update @brillout/vite-plugin-import-build ([2b0f70b](https://github.com/brillout/telefunc/commit/2b0f70b0b0b93707f35bc7adb9383dbdacf76ddb))



## [0.1.66](https://github.com/brillout/telefunc/compare/v0.1.65...v0.1.66) (2023-12-12)


### Bug Fixes

* update @brillout/vite-plugin-import-build ([8f03c81](https://github.com/brillout/telefunc/commit/8f03c814946ae5a706380b4df6eb91d223846434))



## [0.1.65](https://github.com/brillout/telefunc/compare/v0.1.64...v0.1.65) (2023-12-11)


### Bug Fixes

* add types exports ([9c33704](https://github.com/brillout/telefunc/commit/9c3370427b8efaf09ae48487937f21aaf0415c87))
* fix worker export ([0f58135](https://github.com/brillout/telefunc/commit/0f5813589e50493c139f3bb27a95f2ae085437ef))
* improve debug info ([9362a50](https://github.com/brillout/telefunc/commit/9362a50186745cf807b97b57485b417d47e928d0))
* make standalone builds easier (vikejs/vike[#1165](https://github.com/brillout/telefunc/issues/1165), vikejs/vike[#1342](https://github.com/brillout/telefunc/issues/1342)) ([3765c56](https://github.com/brillout/telefunc/commit/3765c5659ad46fe31ea0817f3a65c9a72924ba12))
* update Telefunc's Next.js log ([ee07acb](https://github.com/brillout/telefunc/commit/ee07acb6e6649850a9a26e8e66cbeebe227856ee))
* user app without tsconfig.json ([7d3b0d0](https://github.com/brillout/telefunc/commit/7d3b0d0a757b40a9e27511b9dbb07f6bd6242ac0))



## [0.1.64](https://github.com/brillout/telefunc/compare/v0.1.63...v0.1.64) (2023-11-15)


### Bug Fixes

* don't use tsconfig.json outside of appRootDir ([74590a2](https://github.com/brillout/telefunc/commit/74590a282f485a7b5d64e2683dd6bceffda15644))



## [0.1.63](https://github.com/brillout/telefunc/compare/v0.1.62...v0.1.63) (2023-11-13)


### Bug Fixes

* improve debug info ([ebc352c](https://github.com/brillout/telefunc/commit/ebc352cdf333e1586495c083139a023d3eadab96))



## [0.1.62](https://github.com/brillout/telefunc/compare/v0.1.61...v0.1.62) (2023-09-30)


### Bug Fixes

* support manually importing dist/server/importBuild.cjs ([01b9ea5](https://github.com/brillout/telefunc/commit/01b9ea56586fa176d71cb06da9d02d7b9979e81c))



## [0.1.61](https://github.com/brillout/telefunc/compare/v0.1.60...v0.1.61) (2023-09-21)


### Bug Fixes

* convert config.telefuncFiles to posix paths before validating (fix [#90](https://github.com/brillout/telefunc/issues/90)) ([e16c585](https://github.com/brillout/telefunc/commit/e16c585ec9fa9ecbe8170b8ac30b295003890dab))
* remove assertion that fails on windows ([#90](https://github.com/brillout/telefunc/issues/90)) ([b9d7d0c](https://github.com/brillout/telefunc/commit/b9d7d0ccb69f723db4ead79dbef440a8c0a8caf6))
* update @brillout/import ([#90](https://github.com/brillout/telefunc/issues/90)) ([f0e8bad](https://github.com/brillout/telefunc/commit/f0e8bad6321dd3fc7dc6bb0700b7d62e373bde42))



## [0.1.60](https://github.com/brillout/telefunc/compare/v0.1.59...v0.1.60) (2023-09-11)


### Bug Fixes

* update ts-morph ([dafc8cd](https://github.com/brillout/telefunc/commit/dafc8cda3645fd860a90248f6322390da983b1b2))



## [0.1.59](https://github.com/brillout/telefunc/compare/v0.1.58...v0.1.59) (2023-09-04)


### Bug Fixes

* improve prop paths in serialization error messages (update @brillout/json-serializer) ([92ae904](https://github.com/brillout/telefunc/commit/92ae90492c320ad9ad90f0b76193f86bf3781e97))



## [0.1.58](https://github.com/brillout/telefunc/compare/v0.1.57...v0.1.58) (2023-07-01)


### Bug Fixes

* fix types for /react-streaming export (fix [#76](https://github.com/brillout/telefunc/issues/76)) ([7c84233](https://github.com/brillout/telefunc/commit/7c8423301212732cf6499dd5763f8f1927625017))



## [0.1.57](https://github.com/brillout/telefunc/compare/v0.1.56...v0.1.57) (2023-06-22)


### Bug Fixes

* simplify shield() generation (fix [#75](https://github.com/brillout/telefunc/issues/75)) ([f57cf11](https://github.com/brillout/telefunc/commit/f57cf1113b4a147765591a04501d6a3dde24ffb7))



## [0.1.56](https://github.com/brillout/telefunc/compare/v0.1.55...v0.1.56) (2023-06-14)


### Bug Fixes

* improve telefunction key assertion ([099a212](https://github.com/brillout/telefunc/commit/099a212055e042e75a8eaf1bb4069401ba0a37c7))



## [0.1.55](https://github.com/brillout/telefunc/compare/v0.1.54...v0.1.55) (2023-06-14)


### Bug Fixes

* remove problematic filePath assert() ([3910b18](https://github.com/brillout/telefunc/commit/3910b188e7347bf29cb5b019ad59ed78e15ecfec))
* update @brillout/vite-plugin-import-build ([ded2d80](https://github.com/brillout/telefunc/commit/ded2d809f201f8b98425c5786f90ee138242f79a))



## [0.1.54](https://github.com/brillout/telefunc/compare/v0.1.53...v0.1.54) (2023-06-01)


### Bug Fixes

* tolerate + prefix in collocation convention ([13a3545](https://github.com/brillout/telefunc/commit/13a354579602e3cebfd3ae12c8dd0057afbb0424))



## [0.1.53](https://github.com/brillout/telefunc/compare/v0.1.52...v0.1.53) (2023-04-28)


### Bug Fixes

* use ts-morph project.getSourceFile() instead of glob pattern matching (fix [#71](https://github.com/brillout/telefunc/issues/71)) ([3412ce5](https://github.com/brillout/telefunc/commit/3412ce5ec189b42416cf156933f07a7669759ea3))



## [0.1.52](https://github.com/brillout/telefunc/compare/v0.1.51...v0.1.52) (2023-03-14)


### Bug Fixes

* update @brillout/vite-plugin-import-build ([4b94a83](https://github.com/brillout/telefunc/commit/4b94a83cbbed9269505a20c266df468206df99d6))
* update @brillout/vite-plugin-import-build (fix [#66](https://github.com/brillout/telefunc/issues/66)) ([80f7d56](https://github.com/brillout/telefunc/commit/80f7d56bd4701bf15c594f1a3e21c1148a983bc2))



## [0.1.51](https://github.com/brillout/telefunc/compare/v0.1.50...v0.1.51) (2023-03-05)


### Bug Fixes

* expose error thrown by telefunctions at httpResponse.err (fix [#64](https://github.com/brillout/telefunc/issues/64)) ([bd68427](https://github.com/brillout/telefunc/commit/bd6842744740a045dec193c184040dfcd54b6155))



## [0.1.50](https://github.com/brillout/telefunc/compare/v0.1.49...v0.1.50) (2023-02-25)


### Bug Fixes

* update @brillout/vite-plugin-import-build ([cb850e4](https://github.com/brillout/telefunc/commit/cb850e4e08a066484eaacb19e9241759c1e35272))



## [0.1.49](https://github.com/brillout/telefunc/compare/v0.1.48...v0.1.49) (2023-02-24)


### Bug Fixes

* update @brillout/vite-plugin-import-build ([6763668](https://github.com/brillout/telefunc/commit/67636686cadbde2d25539cc4c57dcf4e6d0fb559))
* update @brillout/vite-plugin-import-build ([bda7a10](https://github.com/brillout/telefunc/commit/bda7a1051ddac3f9c961b318e8531e387e7db191))



## [0.1.48](https://github.com/brillout/telefunc/compare/v0.1.47...v0.1.48) (2023-02-17)


### Bug Fixes

* update @brillout/vite-plugin-import-build ([17ea11a](https://github.com/brillout/telefunc/commit/17ea11ace37f20c6801d721896d3635ce01fb62a))



## [0.1.47](https://github.com/brillout/telefunc/compare/v0.1.46...v0.1.47) (2023-02-14)


### Bug Fixes

* update @brillout/vite-plugin-import-build ([ec5b090](https://github.com/brillout/telefunc/commit/ec5b0903f15fc598705821f023176fa17399c93b))
* update @brillout/vite-plugin-import-build ([90320f4](https://github.com/brillout/telefunc/commit/90320f46e73ccab229ecd9b2595b28fa8ffb0221))



## [0.1.46](https://github.com/brillout/telefunc/compare/v0.1.45...v0.1.46) (2023-02-09)


### Bug Fixes

* update @brillout/vite-plugin-import-build ([#61](https://github.com/brillout/telefunc/issues/61)) ([a07be9f](https://github.com/brillout/telefunc/commit/a07be9fb57bdbc45d341d6fa7ee593a786b5156a))



## [0.1.45](https://github.com/brillout/telefunc/compare/v0.1.44...v0.1.45) (2023-02-09)


### Bug Fixes

* improve error message when client/server out-of-sync ([21fb6ed](https://github.com/brillout/telefunc/commit/21fb6ed8d7c88cbc1dcc3ad08745d85fdae2795d))
* improve naming convention warnings ([#61](https://github.com/brillout/telefunc/issues/61)) ([c886472](https://github.com/brillout/telefunc/commit/c886472093b5c2d9ea865c4744a2941af7789136))
* show posix paths for naming convention warnings ([29d3853](https://github.com/brillout/telefunc/commit/29d38532e216d6d9eef2c1daa673c8256996be19))
* update @brillout/vite-plugin-import-build ([#61](https://github.com/brillout/telefunc/issues/61)) ([ce67cc0](https://github.com/brillout/telefunc/commit/ce67cc0297b524177d7108602dd69543ad8fb8d1))
* update @brillout/vite-plugin-import-build ([#61](https://github.com/brillout/telefunc/issues/61)) ([cd4d619](https://github.com/brillout/telefunc/commit/cd4d619c94bcebf5adfd547648f72fb4e472dc63))
* update @brillout/vite-plugin-import-build (fix workaround) ([d17c18e](https://github.com/brillout/telefunc/commit/d17c18e44b5fe03736314ab9ac9baadf7b665e16))



## [0.1.44](https://github.com/brillout/telefunc/compare/v0.1.43...v0.1.44) (2023-02-07)


### Bug Fixes

* convert SvetelteKit provided paths to posix paths ([#59](https://github.com/brillout/telefunc/issues/59)) ([64b2623](https://github.com/brillout/telefunc/commit/64b2623b95252d3c8c186ea713fb71e7c0645e3c))
* update @brillout/vite-plugin-import-build ([#59](https://github.com/brillout/telefunc/issues/59)) ([947cc61](https://github.com/brillout/telefunc/commit/947cc61b0c36296823a294567b03e183c72295a3))



## [0.1.43](https://github.com/brillout/telefunc/compare/v0.1.42...v0.1.43) (2023-02-02)


### Bug Fixes

* ensure telefunctionReturn is initialized (fix [#58](https://github.com/brillout/telefunc/issues/58)) ([aa7d82e](https://github.com/brillout/telefunc/commit/aa7d82eb376942e0525bf4ded4e80eabfca443c2))



## [0.1.42](https://github.com/brillout/telefunc/compare/v0.1.41...v0.1.42) (2023-01-30)


### Bug Fixes

* update vite-plugin-import-build ([20641d9](https://github.com/brillout/telefunc/commit/20641d9fdd815e145b3687e0db90248ebc006a6d))



## [0.1.41](https://github.com/brillout/telefunc/compare/v0.1.40...v0.1.41) (2023-01-17)


### Bug Fixes

* remove unnecessary and problematic getOutDirs() ([6bce954](https://github.com/brillout/telefunc/commit/6bce95469862b308bdb05c2637ed50e9fb64a813))
* update @brillout/vite-plugin-import-build ([8f5cff2](https://github.com/brillout/telefunc/commit/8f5cff26bc27abfe34aadf5757935734bc5f09ff))



## [0.1.40](https://github.com/brillout/telefunc/compare/v0.1.39...v0.1.40) (2022-11-24)


### Bug Fixes

* don't set outDir when used with Astro (withastro/astro[#5211](https://github.com/brillout/telefunc/issues/5211)) ([6ed9990](https://github.com/brillout/telefunc/commit/6ed99900b5d8c1fddcf53a0e4255e69b051c00db))



## [0.1.39](https://github.com/brillout/telefunc/compare/v0.1.38...v0.1.39) (2022-11-18)


### Bug Fixes

* improve error message upon third-party Vite plugin conflicting with Telefunc ([98cec22](https://github.com/brillout/telefunc/commit/98cec2295d96e900d27cf2cb1330a58d384feb30))
* remove dangling log ([6201ca0](https://github.com/brillout/telefunc/commit/6201ca02e154bce3d31c885c355831a5cf0655ab))



## [0.1.38](https://github.com/brillout/telefunc/compare/v0.1.37...v0.1.38) (2022-11-18)


### Bug Fixes

* manually add Telefunc's rollup input ([146fee9](https://github.com/brillout/telefunc/commit/146fee977e5f396efa5e4c068e7cb24af7c6a395))



## [0.1.37](https://github.com/brillout/telefunc/compare/v0.1.36...v0.1.37) (2022-11-17)


### Bug Fixes

* ensure gobalContext is globally shared ([7dc059d](https://github.com/brillout/telefunc/commit/7dc059d04c818006cb1e15da45d6bf309b8f7b45))
* ensure Telefunc's Vite server middleware is last ([21998d3](https://github.com/brillout/telefunc/commit/21998d32c3227f2f90eeb7638b027f0e3dd10fb9))
* improve error message upon using Telefunc for SSR ([4d17fae](https://github.com/brillout/telefunc/commit/4d17faeea917e46f4a63d2b60112626c49a3e3da))
* set build.outDir only if necessary ([#54](https://github.com/brillout/telefunc/issues/54)) ([db069d2](https://github.com/brillout/telefunc/commit/db069d269b1b7f45263baeb4a2785af3642ce4c1))
* skip checking outDir upon preview ([7629b18](https://github.com/brillout/telefunc/commit/7629b183da57b50514255a27a03741d0b09b9708))



## [0.1.36](https://github.com/brillout/telefunc/compare/v0.1.35...v0.1.36) (2022-11-14)


### Bug Fixes

* implement `context` argument ([d3a6d63](https://github.com/brillout/telefunc/commit/d3a6d636dfb02a3c124beb2013d3e9d93e2cc858))



## [0.1.35](https://github.com/brillout/telefunc/compare/v0.1.34...v0.1.35) (2022-11-14)


### Bug Fixes

* normalize rollupOptions.input ([fd034ed](https://github.com/brillout/telefunc/commit/fd034ed81a81de8e4d5153b3759cfa7adf407289))
* remove @brillout/json-serializer from optimizeDeps.exclude (fix [#53](https://github.com/brillout/telefunc/issues/53)) ([11f6ed6](https://github.com/brillout/telefunc/commit/11f6ed6fb1c8329b6cab226169cffe6f11022a0b))



## [0.1.34](https://github.com/brillout/telefunc/compare/v0.1.33...v0.1.34) (2022-11-12)


### Bug Fixes

* avoid importBuild var collision ([7d67401](https://github.com/brillout/telefunc/commit/7d67401f883a30b84fec3cf589e57d1560d557d8))
* improve HTTP response upon telefunction not found ([4ffe81f](https://github.com/brillout/telefunc/commit/4ffe81f0aafb6e9b1b939b98773d2a4b83657df1))



## [0.1.33](https://github.com/brillout/telefunc/compare/v0.1.32...v0.1.33) (2022-11-07)


### Bug Fixes

* implement `onAbort()` and soft-deprecate `onTelefunctionRemoteCallError()` ([ff15341](https://github.com/brillout/telefunc/commit/ff153416f5eac4a8ce295219c50762f4d37ebc0b))
* rename `telefuncConfig` to `config` ([01695da](https://github.com/brillout/telefunc/commit/01695da3a71098483949e95ddad06112fd7ca1ff))
* simplify handling of isomorphic code ([5457a30](https://github.com/brillout/telefunc/commit/5457a303c0979cb63ec9ade5abdc62739367d8b1))



## [0.1.32](https://github.com/brillout/telefunc/compare/v0.1.31...v0.1.32) (2022-11-03)


### Bug Fixes

* let Vite discover nested dependency upon pre-optimizing ([164afd3](https://github.com/brillout/telefunc/commit/164afd38b591d500805b0f72fe66eec1905b59f3))
* update `@brillout/json-serializer` ([755a8ba](https://github.com/brillout/telefunc/commit/755a8ba65a2a4f625f57446a57b213e500f782b7))



## [0.1.31](https://github.com/brillout/telefunc/compare/v0.1.30...v0.1.31) (2022-11-03)


### Features

* allow server config to be defined with `vite.config.js` ([41440e2](https://github.com/brillout/telefunc/commit/41440e2bdce0693dc72e9f555e9889dbc0db70f8))



## [0.1.30](https://github.com/brillout/telefunc/compare/v0.1.29...v0.1.30) (2022-11-03)


### Bug Fixes

* workaround Vite bug that wrongfully pre-optimizes `telefunc` module ([7ead64b](https://github.com/brillout/telefunc/commit/7ead64bf1b09571eb753d587bcdc7b1cec7420dc))



## [0.1.29](https://github.com/brillout/telefunc/compare/v0.1.28...v0.1.29) (2022-11-02)


### Bug Fixes

* add naming convention warnings ([37ab2bf](https://github.com/brillout/telefunc/commit/37ab2bf8f3b12a5a58b95aeda5fc1da82da31e37))
* fix wrong error message ([e8d5b45](https://github.com/brillout/telefunc/commit/e8d5b45906e7e0402fd19dbfe3100a48d5a91090))
* improve dynamic loading of modules ([972f8c2](https://github.com/brillout/telefunc/commit/972f8c218ba13b3488c26627da726c0e40e851f6))
* improve wrong config error messages ([d6b5998](https://github.com/brillout/telefunc/commit/d6b5998ba637f71ad0a03d9d8498182dae2eef4a))
* improve wrong usage message ([2fa05d5](https://github.com/brillout/telefunc/commit/2fa05d542905d1233d1e275a3a4b3d6d604e1e52))


### Performance Improvements

* lazy-load telefunc files ([87ec5d2](https://github.com/brillout/telefunc/commit/87ec5d27a1b15affe4fa6d07569ef5c1f9663d0f))



## [0.1.28](https://github.com/brillout/telefunc/compare/v0.1.27...v0.1.28) (2022-10-26)


### Bug Fixes

* stop assuming UserConfig.build.outDir to be a posix path ([013c2fc](https://github.com/brillout/telefunc/commit/013c2fc56b62e2c5328f4b6dc54a580ef9075e21))



## [0.1.27](https://github.com/brillout/telefunc/compare/v0.1.26...v0.1.27) (2022-10-24)


### Bug Fixes

* add shield() codegen to webpack loaders (fix [#35](https://github.com/brillout/telefunc/issues/35)) ([79cb772](https://github.com/brillout/telefunc/commit/79cb772eff160a62834e462418c54c1463ed9504))
* improve DX upon server errors ([4321598](https://github.com/brillout/telefunc/commit/4321598321f4dd1144d738f7711164457748d653))
* improve shield() gen logs ([adf13ed](https://github.com/brillout/telefunc/commit/adf13ed0f59776b7ba82ce1f729fb5d03e84af2e))
* improve shield() generation ([de0996e](https://github.com/brillout/telefunc/commit/de0996eccdbb3da65c761c92cbdc1936f1260a7b))
* remove superfluous `isProduction` config and improve `isProduction()` check ([a166c58](https://github.com/brillout/telefunc/commit/a166c5845928cb6287949f285f7d6a49fd80bcc2))
* use strings instead of symbols as keys ([bdbd31e](https://github.com/brillout/telefunc/commit/bdbd31e11a734526d4fa033a896c525d82ee82a8))


### Performance Improvements

* [Vite] lazy load `.telefunc.js` files ([dcf1423](https://github.com/brillout/telefunc/commit/dcf1423fb10bd82b6df887d36b78041a7f84abe3))



## [0.1.26](https://github.com/brillout/telefunc/compare/v0.1.25...v0.1.26) (2022-10-10)


### Bug Fixes

* gracefully handle `.telefunc.js` non-function exports ([9eacb25](https://github.com/brillout/telefunc/commit/9eacb258d4ec11e1323da5029076050eefd25568))
* improve DX around erroneous context setup ([a33d89b](https://github.com/brillout/telefunc/commit/a33d89b6b15578454c196afbd802a8fc7c70db2f))
* improve error handling ([1dc21e2](https://github.com/brillout/telefunc/commit/1dc21e24ddebf48d2bac28e6c1e4b249dd279a5c))
* improve error message upon telefunction not found ([cb6f7a8](https://github.com/brillout/telefunc/commit/cb6f7a8895738fe6ce994ed9d7b350688ec7fd88))
* improve error message upon wrong `getContext()` usage ([0a0ac4e](https://github.com/brillout/telefunc/commit/0a0ac4e71f816b838cb759d7a78741a6fb0376d2))


### Features

* add SSR context support React hooks ([17e79d8](https://github.com/brillout/telefunc/commit/17e79d8826b40cf6110819b0afd284acdc8af75a))
* new React hook `useData()` enabling using Telefunc for SSR data fetching ([7d8a757](https://github.com/brillout/telefunc/commit/7d8a757ffa6afa0be051cdf4f60440b2fc733927))


### Performance Improvements

* stop auto-generating `shiled()` in dev ([32a3008](https://github.com/brillout/telefunc/commit/32a3008a77c2c61713135c0c9490db196a05d91a))



## [0.1.25](https://github.com/brillout/telefunc/compare/v0.1.24...v0.1.25) (2022-09-05)


### Bug Fixes

* improve config.build.outDir handling ([c639a34](https://github.com/brillout/telefunc/commit/c639a349fce8ae0757bf3d23680261bbb8a99830))
* support ReScript ([3d5b646](https://github.com/brillout/telefunc/commit/3d5b64605285b76ac30f5f38304bd22e8181ded9))
* support Vite 3 and latest vite-plugin-ssr version ([0a75971](https://github.com/brillout/telefunc/commit/0a75971f9e16ca35a8b526e4ce84b5eef3ca7080))


### Features

* support Vite CLI ([68c7cb6](https://github.com/brillout/telefunc/commit/68c7cb6c14328fcd5e6306b43ffc1b18dedfb501))



## [0.1.24](https://github.com/brillout/telefunc/compare/v0.1.23...v0.1.24) (2022-08-02)


### Bug Fixes

* [`shield()` auto generator] support latest TypeScript version ([a9fba17](https://github.com/brillout/telefunc/commit/a9fba1782419104914d1beeb272a8d101a38246c))



## [0.1.23](https://github.com/brillout/telefunc/compare/v0.1.22...v0.1.23) (2022-07-31)


### Bug Fixes

* `dist/server/package.json` generation (fix [#33](https://github.com/brillout/telefunc/issues/33)) ([ccfc607](https://github.com/brillout/telefunc/commit/ccfc607642d94523d7310b912fdbfbefdc5034d9))
* add worker exports ([#32](https://github.com/brillout/telefunc/issues/32)) ([d6c70e8](https://github.com/brillout/telefunc/commit/d6c70e894cf74505b641c16e6557f7fd0ee7f3e9))
* tolerate missing __dirname (fix [#32](https://github.com/brillout/telefunc/issues/32)) ([73f3862](https://github.com/brillout/telefunc/commit/73f3862d026301932f7555640f7068037306940f))
* treat `Object.ceate(null)` as plain JavaScript object ([4c3c163](https://github.com/brillout/telefunc/commit/4c3c1636c07f57f542916f596ea9ce6c5a4d0848))



## [0.1.22](https://github.com/brillout/telefunc/compare/v0.1.21...v0.1.22) (2022-05-10)


### Features

* `onBug()` ([2b1be00](https://github.com/brillout/telefunc/commit/2b1be008969e63d7a2e8988058085a42d9bb04d4))



## [0.1.21](https://github.com/brillout/telefunc/compare/v0.1.20...v0.1.21) (2022-04-30)


### Bug Fixes

* improve shield error message ([4188010](https://github.com/brillout/telefunc/commit/41880108d3a9744c83083933e57ebc59306602e8))



## [0.1.20](https://github.com/brillout/telefunc/compare/v0.1.19...v0.1.20) (2022-04-22)


### Features

* Use TypeScript to automatically generate `shield()` ([telefunc.com > TypeScript > `shield()`](https://telefunc.com/typescript#shield), [#23](https://github.com/brillout/telefunc/issues/23), [#25](https://github.com/brillout/telefunc/pull/25))




## [0.1.19](https://github.com/brillout/telefunc/compare/v0.1.18...v0.1.19) (2022-03-07)


### Bug Fixes

* fix TS resolve helpers ([09b2700](https://github.com/brillout/telefunc/commit/09b2700a1caec00b0e3c5e3c7cc4c5e19d11653e))



## [0.1.18](https://github.com/brillout/telefunc/compare/v0.1.17...v0.1.18) (2022-03-05)


### Bug Fixes

* isFileAlreadyTransformed check and move it to separate fn ([e26b015](https://github.com/brillout/telefunc/commit/e26b0151c6a04280d7c9fc3c70a873a289fe2a09))



## [0.1.17](https://github.com/brillout/telefunc/compare/v0.1.16...v0.1.17) (2022-03-03)


### Bug Fixes

* allow `.telefunc.js` ESM files to be loaded directly ([0f14491](https://github.com/brillout/telefunc/commit/0f144917c28dd253ab3fe8955af22005d11631c6))
* fix erroneous Vite stack assumption ([91ba118](https://github.com/brillout/telefunc/commit/91ba1184ebd5a48e9824d2698c38f6c804f19c4f))
* improve error message ([13011de](https://github.com/brillout/telefunc/commit/13011def3712a4e5a95b68ef79e186fb1accff93))
* support client resolving for legacy toolchains ([4f3d414](https://github.com/brillout/telefunc/commit/4f3d41400510b16ecd4ec1ce02145946d1965fcc))
* support Expo/Metro resolver ([6ede743](https://github.com/brillout/telefunc/commit/6ede7432b9b45037b11f1725a0d02e9115ef0866))


### Features

* allow user to manually provide the telefunc files with `telefuncConfig.telefuncFiles` ([b47f0d9](https://github.com/brillout/telefunc/commit/b47f0d91293fe3946f808885ebfb0aaa652f7822))
* telefunc babel plugin ([c8e46df](https://github.com/brillout/telefunc/commit/c8e46dfa14d7e5ef7fbeffd694b58cca8d72d4a5))



## [0.1.16](https://github.com/brillout/telefunc/compare/v0.1.15...v0.1.16) (2022-02-28)


### Bug Fixes

* remove tests from npm package ([c10c501](https://github.com/brillout/telefunc/commit/c10c50119cd81b1401dc937e235e73aa410ae749))


### Features

* `telefuncConfig.httpHeaders` ([28993e1](https://github.com/brillout/telefunc/commit/28993e1be034f783caec5236847c6bf0d40a9e50))



## [0.1.15](https://github.com/brillout/telefunc/compare/v0.1.14...v0.1.15) (2022-02-18)


### Bug Fixes

* fix @vercel/ncc bundling ([2d5532c](https://github.com/brillout/telefunc/commit/2d5532cdcd78cf568a411d8497805b171c24b30f))



## [0.1.14](https://github.com/brillout/telefunc/compare/v0.1.13...v0.1.14) (2022-02-17)


### Bug Fixes

* remove debug log ([40b486b](https://github.com/brillout/telefunc/commit/40b486be4fc5c5d8f1fe3d37c17c63e2e763574e))



## [0.1.13](https://github.com/brillout/telefunc/compare/v0.1.12...v0.1.13) (2022-02-17)


### Bug Fixes

* avoid duplicated code in importBuild.js ([eae771e](https://github.com/brillout/telefunc/commit/eae771ec0b553bd691b49cb3759279f1b19476c6))
* improve deploy DX ([ddba37e](https://github.com/brillout/telefunc/commit/ddba37e975e5b8ec7df8b981df512eeed2052878))
* improve error message ([adedcdf](https://github.com/brillout/telefunc/commit/adedcdf816d7a7ed4eb6bb0debe0cbacdc4ffd7e))
* improve no telefunc file found error ([51fff0a](https://github.com/brillout/telefunc/commit/51fff0adfb46938f82cd6e0443506dc1284f5f42))
* improve server-side test [cloudfalre workers] ([e847c10](https://github.com/brillout/telefunc/commit/e847c100c6388b1e186f729a114b8cb6ff912380))
* relative window path ([102b2a9](https://github.com/brillout/telefunc/commit/102b2a9b1c42610b1a0f424682c6e81b707d6c3a))
* remove etag generation [cloudflare workers] ([850acb7](https://github.com/brillout/telefunc/commit/850acb7272e1c85f4f3da884a0938f33a6f3a5f5))
* remove eval usage for cloudfalre workers ([ca2401c](https://github.com/brillout/telefunc/commit/ca2401ca915a90bd2d08dcb02d386365e285cc7d))
* warn user upon wrong telefunc() usage ([e0a5987](https://github.com/brillout/telefunc/commit/e0a5987fd6b29a24822f9f0e5148a41aae2d2878))


### Reverts

* remove unused generated dist/server/package.json ([b72e908](https://github.com/brillout/telefunc/commit/b72e9081280d7f1c72401e9074d2132756014dce))



## [0.1.12](https://github.com/brillout/telefunc/compare/v0.1.11...v0.1.12) (2022-02-13)


### Bug Fixes

* add @brillout/json-s to pre-bundling ([edb37a4](https://github.com/brillout/telefunc/commit/edb37a4dbd65fd3152ea8053e3dcf8a2a9b47932))
* remove unecessary `optimizeDeps.exclude` entries ([c324a18](https://github.com/brillout/telefunc/commit/c324a18510fa76abcf5542723a49c5dc2e7fd0d8))


### Features

* add debug flag ([c4ed79d](https://github.com/brillout/telefunc/commit/c4ed79da40afec708ea86a422bf39b56d85a0338))



## [0.1.11](https://github.com/brillout/telefunc/compare/v0.1.10...v0.1.11) (2022-02-13)


### Bug Fixes

* automatically retrieve `viteDevServer` ([d8af512](https://github.com/brillout/telefunc/commit/d8af5122bce12bc7a8f5417b4d3a7d5f578328a2))
* improve error message upon wrong configuration ([ff19628](https://github.com/brillout/telefunc/commit/ff196287342b4ea3addbc0e0a559eef680f73796))
* improve overall bundler handling logic ([c12ed9b](https://github.com/brillout/telefunc/commit/c12ed9b53404244cc10fc20af17176cc30224757))



## [0.1.10](https://github.com/brillout/telefunc/compare/v0.1.9...v0.1.10) (2022-02-10)


### Features

* allow users to directly use the webpack loader without framework ([aeeeed6](https://github.com/brillout/telefunc/commit/aeeeed6fe82f8adb6b7061b3d27c3c7b4772ccc0))



## [0.1.9](https://github.com/brillout/telefunc/compare/v0.1.8...v0.1.9) (2022-02-09)


### Bug Fixes

* do not use `require.resolve()` in dev ([614a1e8](https://github.com/brillout/telefunc/commit/614a1e8ec71d21a22874a3baabf9b422810d3322))



## [0.1.8](https://github.com/brillout/telefunc/compare/v0.1.7...v0.1.8) (2022-02-08)


### Bug Fixes

* circumvent `moduleExists()` Vite bug ([32da7fd](https://github.com/brillout/telefunc/commit/32da7fdcb02fc957d67c66a84b9bb850bd4eb861))



## [0.1.7](https://github.com/brillout/telefunc/compare/v0.1.6...v0.1.7) (2022-02-05)


### Bug Fixes

* improve DX around not found telefunction ([c40a378](https://github.com/brillout/telefunc/commit/c40a378513f048090f55e24e5d3a04c16e49f76f))



## [0.1.6](https://github.com/brillout/telefunc/compare/v0.1.5...v0.1.6) (2022-02-03)


### Bug Fixes

* improve DX around malformed Telefunc request in dev ([c4b2dd7](https://github.com/brillout/telefunc/commit/c4b2dd7899ff2cce26a823c6f119c9454bb0f7e8))
* improve DX upon wrong HTTP request body ([f8751f0](https://github.com/brillout/telefunc/commit/f8751f0500d1d9842942a2d10a140fc3e59a2bb1))
* improve telefunction's human readable name ([c8dd213](https://github.com/brillout/telefunc/commit/c8dd21334a963c5a77ae13199fb3f9da92708641))
* warn user when telefunction has no `shield()` ([dc08539](https://github.com/brillout/telefunc/commit/dc08539e5a468f24747f8467063bfe09117d2188))



## [0.1.5](https://github.com/brillout/telefunc/compare/v0.1.4...v0.1.5) (2022-02-01)


### Bug Fixes

* improve DX upon wrong `shield()` usage ([9f8527b](https://github.com/brillout/telefunc/commit/9f8527b0d090cb776cec7292c0e1c7e903eb9d2f))


### Features

* allow `shield()` arguments to be passed in reversed order: `shield([ t.type.string ], myTelefunction)` ([1f3368f](https://github.com/brillout/telefunc/commit/1f3368fe0042ba15cdb47ba654a0ce1430b2624b))



## [0.1.4](https://github.com/brillout/telefunc/compare/v0.1.3...v0.1.4) (2022-01-29)


### Bug Fixes

* include `node/vite/importTelefuncFiles.ts` in npm package (fix [#16](https://github.com/brillout/telefunc/issues/16)) ([3eeafa3](https://github.com/brillout/telefunc/commit/3eeafa3ce02b304702d92ae993e6b275645195ac))



## [0.1.3](https://github.com/brillout/telefunc/compare/v0.1.2...v0.1.3) (2022-01-28)


### Bug Fixes

* catch problematic `Abort()` typos ([d887886](https://github.com/brillout/telefunc/commit/d88788680dfe1200137a6457f2ae17b952e89e03))
* ensure telefunctions to not throw a primitve as error ([14698fc](https://github.com/brillout/telefunc/commit/14698fca546e9fc0a7c856ae6fd08f190f797b31))
* improve DX upon serialization failure ([5656511](https://github.com/brillout/telefunc/commit/5656511acff7b8d8b948888e8269b9e771e25c0a))
* reduce npm pcakage size ([e8643f8](https://github.com/brillout/telefunc/commit/e8643f83f987c5bcecb838f27713e96567ce83dc))
* register remote call error listeners on the global scope ([19e5b26](https://github.com/brillout/telefunc/commit/19e5b26b29ac568371f4c4b662b73f904d53d86f))



## [0.1.2](https://github.com/brillout/telefunc/compare/v0.1.1...v0.1.2) (2022-01-25)


### Bug Fixes

* improve TelefunctionError type ([41a572a](https://github.com/brillout/telefunc/commit/41a572af57a7ba48533aa97e0883204a63ccda16))
* make error handling consistent between remote call and SSR call ([10edb6a](https://github.com/brillout/telefunc/commit/10edb6ad0cff1db3ba5933307ed21ba261019317))



## [0.1.1](https://github.com/brillout/telefunc/compare/0.1.0...0.1.1) (2022-01-24)


### Bug Fixes

* enable isomorphic imports by refactoring source code file structure and adopting new TS/ESM/CJS strategy ([d0c182d](https://github.com/brillout/telefunc/commit/d0c182d769b68368c2fe59c0771ed0b1a6f3b60c))
* warn user upon incorrect usage of isomorphic imports ([72700ca](https://github.com/brillout/telefunc/commit/72700ca3899f77779e55cf400a1c81206fbea095))



# Telefunc (new 2021/2022 version) `0.1.0`

Initial release.

# Telefunc (old 2020 version) `0.0.26`

See [github.com/brillout/telefunc-old](https://github.com/brillout/telefunc-old).

# Wildcard API `v0.5.3`

See [github.com/brillout/wildcard-api](https://github.com/brillout/wildcard-api).
