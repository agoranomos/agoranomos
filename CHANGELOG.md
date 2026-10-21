# Changelog

## [0.2.1](https://github.com/agoranomos/agoranomos/compare/v0.2.0...v0.2.1) (2026-10-21)


### Performance Improvements

* **airflow:** lower the memory limit of the DAG processor from 768 to 512 MiB ([#105](https://github.com/agoranomos/agoranomos/issues/105)) ([3c91f71](https://github.com/agoranomos/agoranomos/commit/3c91f714e61271a317c87624e490b516e2a42e2f))


### Documentation

* **adr:** accept ADR-0030 ([#106](https://github.com/agoranomos/agoranomos/issues/106)) ([771e2d1](https://github.com/agoranomos/agoranomos/commit/771e2d164ec1d0ddd5d3b90d3e2ad6b53fd16a76))
* **adr:** propose ADR-0030, three MCP servers in v0.3.0 with the stdio servers in local mode ([#104](https://github.com/agoranomos/agoranomos/issues/104)) ([e56a77e](https://github.com/agoranomos/agoranomos/commit/e56a77e1055ff7462aca60e59e27b9ac0bad41bf))
* say that Phase 2 is released as v0.2.0 ([#101](https://github.com/agoranomos/agoranomos/issues/101)) ([0fb0c48](https://github.com/agoranomos/agoranomos/commit/0fb0c48d12521634800a96faec04855de3503746))

## [0.2.0](https://github.com/agoranomos/agoranomos/compare/v0.1.5...v0.2.0) (2026-10-21)


### Features

* **airflow:** load the dbt project into Cosmos from a manifest, and run dbt tasks in a pool ([#79](https://github.com/agoranomos/agoranomos/issues/79)) ([595daf9](https://github.com/agoranomos/agoranomos/commit/595daf93624987bf049284c522b48cde99882be0))
* **dbt:** model advertising statistics once, at the grain of a campaign and a day, with a reporting currency ([#94](https://github.com/agoranomos/agoranomos/issues/94)) ([e8fcc17](https://github.com/agoranomos/agoranomos/commit/e8fcc17cc5e0356a13c2119998670fc24b5b2a09))
* **demo:** load the history of four fictional ad platforms and a source of rates ([#82](https://github.com/agoranomos/agoranomos/issues/82)) ([0064de6](https://github.com/agoranomos/agoranomos/commit/0064de604a3b352c418f7ee712d4dddb1f4276e3))
* **demo:** load the new days of the demo every day ([#97](https://github.com/agoranomos/agoranomos/issues/97)) ([4c3872d](https://github.com/agoranomos/agoranomos/commit/4c3872dccc3aeda94111122eaeb122a0b9cc861e))
* **metabase:** keep the content of the package in Metabase as code, view-only for everyone but admins ([#83](https://github.com/agoranomos/agoranomos/issues/83)) ([20f7877](https://github.com/agoranomos/agoranomos/commit/20f78779450782618061161c8ec4157780373f72))
* **metabase:** show the demo in two dashboards with filters ([#96](https://github.com/agoranomos/agoranomos/issues/96)) ([3ecd5c1](https://github.com/agoranomos/agoranomos/commit/3ecd5c13881513c77940d824ce5c41727e1ced17))
* **ops:** record the runs of dbt with elementary and show the pipeline in an Ops collection ([#98](https://github.com/agoranomos/agoranomos/issues/98)) ([15630be](https://github.com/agoranomos/agoranomos/commit/15630bebb9ccab6172510b06230e5cf8a724b0f2))


### Performance Improvements

* **airflow:** ping the Celery worker without starting Airflow ([#84](https://github.com/agoranomos/agoranomos/issues/84)) ([55c343e](https://github.com/agoranomos/agoranomos/commit/55c343e0f3305d226c61b2f7cf0efb4e78a20a6f))


### Documentation

* accept ADR-0027 ([#81](https://github.com/agoranomos/agoranomos/issues/81)) ([c6598b3](https://github.com/agoranomos/agoranomos/commit/c6598b39419eb7adb1db04286893e6b9d85cde8c))
* **adr:** accept ADR-0028 ([#95](https://github.com/agoranomos/agoranomos/issues/95)) ([24fd5d1](https://github.com/agoranomos/agoranomos/commit/24fd5d1660bda9a0ed36342b3fb35ceb09d7a144))
* **adr:** accept ADR-0029 ([#100](https://github.com/agoranomos/agoranomos/issues/100)) ([2140229](https://github.com/agoranomos/agoranomos/commit/2140229b97cf2f6967138df78975752386071a6c))
* **tutorials:** add the tutorial Run Agoranomos locally with demo data ([#99](https://github.com/agoranomos/agoranomos/issues/99)) ([e1f8ce8](https://github.com/agoranomos/agoranomos/commit/e1f8ce888d78d53c559d774ccd5769f8f1ba1e90))

## [0.1.5](https://github.com/agoranomos/agoranomos/compare/v0.1.4...v0.1.5) (2026-10-13)


### Bug Fixes

* **airflow:** keep the metadata database and the signing key away from the Celery worker, and give its results a database of their own ([#78](https://github.com/agoranomos/agoranomos/issues/78)) ([fb27018](https://github.com/agoranomos/agoranomos/commit/fb27018e5e22b1b777e52bb3295e77381b84babf))


### Documentation

* accept ADR-0024 and ADR-0025 ([#75](https://github.com/agoranomos/agoranomos/issues/75)) ([a4f4d2d](https://github.com/agoranomos/agoranomos/commit/a4f4d2d93a801ce343a72ce5f7c795729e28a911))
* decide that one deployment is one business, and model advertising statistics once ([#73](https://github.com/agoranomos/agoranomos/issues/73)) ([9c932bc](https://github.com/agoranomos/agoranomos/commit/9c932bcb1fb6da85f4664ea5991e8ca719c502b0))

## [0.1.4](https://github.com/agoranomos/agoranomos/compare/v0.1.3...v0.1.4) (2026-10-12)


### Performance Improvements

* **airflow:** check the DAG processor and the triggerer cheaply, and parse the DAG files every five minutes ([#68](https://github.com/agoranomos/agoranomos/issues/68)) ([3c66d2b](https://github.com/agoranomos/agoranomos/commit/3c66d2b896c71ad72b322853e9c38a83a5bc56f5))

## [0.1.3](https://github.com/agoranomos/agoranomos/compare/v0.1.2...v0.1.3) (2026-10-12)


### Documentation

* serve the site without third-party hosts ([#66](https://github.com/agoranomos/agoranomos/issues/66)) ([7577a81](https://github.com/agoranomos/agoranomos/commit/7577a819b701bab0c6039410c4f43841a306ac83))

## [0.1.2](https://github.com/agoranomos/agoranomos/compare/v0.1.1...v0.1.2) (2026-10-12)


### Bug Fixes

* **backups:** take a manual backup into the folder of .env after it changed ([#63](https://github.com/agoranomos/agoranomos/issues/63)) ([782c5d6](https://github.com/agoranomos/agoranomos/commit/782c5d6037a5823b32eded69a962185b5aab9d9e))

## [0.1.1](https://github.com/agoranomos/agoranomos/compare/v0.1.0...v0.1.1) (2026-10-12)


### Documentation

* say that Phase 1 is released as v0.1.0 ([#61](https://github.com/agoranomos/agoranomos/issues/61)) ([4bd84d9](https://github.com/agoranomos/agoranomos/commit/4bd84d9b9b59a68b2d337e5fc7f1e613cca72c7f))

## 0.1.0 (2026-10-11)


### Features

* add ./agoranomos with init and preflight ([#15](https://github.com/agoranomos/agoranomos/issues/15)) ([e058f3d](https://github.com/agoranomos/agoranomos/commit/e058f3dc6e16df8a29d722487ee9a74485a60093))
* add Airflow with LocalExecutor by default and a Celery overlay ([#18](https://github.com/agoranomos/agoranomos/issues/18)) ([d0980f8](https://github.com/agoranomos/agoranomos/commit/d0980f83f1f00e72fe00e95a0966ce6d00e9d1e2))
* add Metabase and its first-start setup ([#19](https://github.com/agoranomos/agoranomos/issues/19)) ([48094f7](https://github.com/agoranomos/agoranomos/commit/48094f7f376a4cfdb00940826bfcab26b297b727))
* add server mode, with Metabase behind Caddy on ports 80 and 443 only ([#51](https://github.com/agoranomos/agoranomos/issues/51)) ([58ca054](https://github.com/agoranomos/agoranomos/commit/58ca05416e9d051520c69f51819fd54563238634))
* add the loader and transformer roles of the warehouse ([#21](https://github.com/agoranomos/agoranomos/issues/21)) ([3fd72f7](https://github.com/agoranomos/agoranomos/commit/3fd72f76e6d41efe771749d76d172059811e9c77))
* add the PostgreSQL instances and the stack tests ([#17](https://github.com/agoranomos/agoranomos/issues/17)) ([f969e8a](https://github.com/agoranomos/agoranomos/commit/f969e8a2679c190b627402b02f18505025234ee0))
* apply the brand style to the command line, the demo, the docs site and the README ([#46](https://github.com/agoranomos/agoranomos/issues/46)) ([a630f69](https://github.com/agoranomos/agoranomos/commit/a630f6941dc844386139efa0e2d12fe4a92d3cea))
* back up the databases every day, and restore them with one command ([#55](https://github.com/agoranomos/agoranomos/issues/55)) ([4405b4b](https://github.com/agoranomos/agoranomos/commit/4405b4b64a69aba103e594c8196e1b964fd08643))
* build the Airflow image with dlt, Cosmos and dbt ([#20](https://github.com/agoranomos/agoranomos/issues/20)) ([ad85c7f](https://github.com/agoranomos/agoranomos/commit/ad85c7fd67c46f13d4ee8dd0b55c15e790ce39a5))
* build the marts with dbt through Cosmos ([#23](https://github.com/agoranomos/agoranomos/issues/23)) ([f623ea7](https://github.com/agoranomos/agoranomos/commit/f623ea73eec5cd3a4f0d02988325885ae301796a))
* describe every model and column in dbt ([#29](https://github.com/agoranomos/agoranomos/issues/29)) ([a63ab76](https://github.com/agoranomos/agoranomos/commit/a63ab76abe5ad68511e9e202254fb886e106b6c1))
* give every service a measured memory limit, and describe local mode as built ([#39](https://github.com/agoranomos/agoranomos/issues/39)) ([0744c29](https://github.com/agoranomos/agoranomos/commit/0744c29037be5badd938f61da6f6eb4a512c3c2b))
* give the network of the stack IPv6 in server mode, and keep the scheduler alive at the memory limit ([#58](https://github.com/agoranomos/agoranomos/issues/58)) ([e1cfb12](https://github.com/agoranomos/agoranomos/commit/e1cfb12ef796873259bc6e54eec70c1f0135ad61))
* load demo ad statistics with dlt ([#22](https://github.com/agoranomos/agoranomos/issues/22)) ([f4ecf5a](https://github.com/agoranomos/agoranomos/commit/f4ecf5af6f148ca02d4cee2a350b579ea82562aa))
* make the demo ready on the first start ([#24](https://github.com/agoranomos/agoranomos/issues/24)) ([9d453f5](https://github.com/agoranomos/agoranomos/commit/9d453f509f3e6d836691571618927f229863037b))
* make the kernel stop a task before the scheduler at the memory limit ([#40](https://github.com/agoranomos/agoranomos/issues/40)) ([c5078d0](https://github.com/agoranomos/agoranomos/commit/c5078d041648553917b19e36f1172a08134de286))
* print the Agoranomos banner in up and help ([#27](https://github.com/agoranomos/agoranomos/issues/27)) ([b559e79](https://github.com/agoranomos/agoranomos/commit/b559e79ff5c4447e63ac4e29a6823dc6e7642270))
* push the metadata of the marts to Metabase, with a deployment's overrides ([#30](https://github.com/agoranomos/agoranomos/issues/30)) ([8282e44](https://github.com/agoranomos/agoranomos/commit/8282e447fe3abef9adad1d2d9217dc4c648a87e7))
* read .env as Compose does, limit metabase_reader, and budget the memory by variant and mode ([#49](https://github.com/agoranomos/agoranomos/issues/49)) ([39fc772](https://github.com/agoranomos/agoranomos/commit/39fc7726059d9fc58c9c61a88fdc8103028b3d58))
* sign the sessions of Metabase, keep sign-in addresses at home, and add credentials ([#35](https://github.com/agoranomos/agoranomos/issues/35)) ([dbad25f](https://github.com/agoranomos/agoranomos/commit/dbad25f110367ac9dd3b67a1723f2ab63134fc0c))
* start no service as root, drop every capability, and close the platform databases ([#38](https://github.com/agoranomos/agoranomos/issues/38)) ([47e077c](https://github.com/agoranomos/agoranomos/commit/47e077c22bcf46b2ec1185fd542a0d4c9f5f044e))


### Bug Fixes

* **backups:** trap a hang-up in restore, give platform-db 512 MiB, and accept ADR-0020 ([#56](https://github.com/agoranomos/agoranomos/issues/56)) ([e6a1279](https://github.com/agoranomos/agoranomos/commit/e6a1279b7298d98070854e6fe9a412e03e18b6a0))
* close the findings of the final review of the mart metadata ([#31](https://github.com/agoranomos/agoranomos/issues/31)) ([73784a3](https://github.com/agoranomos/agoranomos/commit/73784a36b902cc8f6d665195da0d7c3afa1bdb3a))
* close the findings of the review of the demo path ([#28](https://github.com/agoranomos/agoranomos/issues/28)) ([9e4d616](https://github.com/agoranomos/agoranomos/commit/9e4d6169d1a4c3002aeea918b3152a8bd1505a2b))


### Documentation

* accept ADR-0013 and ADR-0014 ([#14](https://github.com/agoranomos/agoranomos/issues/14)) ([c0940bb](https://github.com/agoranomos/agoranomos/commit/c0940bbd82756f80ed4c45723728ee5fa438219d))
* accept ADR-0016 ([#32](https://github.com/agoranomos/agoranomos/issues/32)) ([6bb7c1e](https://github.com/agoranomos/agoranomos/commit/6bb7c1e67ed72258af092a21742c44b7d2158193))
* accept ADR-0017 and ADR-0018 ([#45](https://github.com/agoranomos/agoranomos/issues/45)) ([de56e52](https://github.com/agoranomos/agoranomos/commit/de56e522e99e1aa940e6d6d4160cb89b94bf860b))
* accept ADR-0019 and ADR-0021 ([#52](https://github.com/agoranomos/agoranomos/issues/52)) ([a032d5f](https://github.com/agoranomos/agoranomos/commit/a032d5f8c536baa9678ba6add7143276ecff2e27))
* accept ADR-0022 ([#50](https://github.com/agoranomos/agoranomos/issues/50)) ([ffd3454](https://github.com/agoranomos/agoranomos/commit/ffd3454944e8c393c98940cf18639631854fe487))
* add ADR-0013 on extensions and ADR-0014 on starting the stack ([#12](https://github.com/agoranomos/agoranomos/issues/12)) ([eb2ebcf](https://github.com/agoranomos/agoranomos/commit/eb2ebcfaf1e14fffe654a184e267b4a49179f64a))
* add ADRs 0002–0012 ([#3](https://github.com/agoranomos/agoranomos/issues/3)) ([4f1ed3a](https://github.com/agoranomos/agoranomos/commit/4f1ed3af15ef5049e05e1e8088fa0266c63c80ef))
* add community files, AGENTS.md and the full README ([#4](https://github.com/agoranomos/agoranomos/issues/4)) ([ac82d75](https://github.com/agoranomos/agoranomos/commit/ac82d7510c30beee344fd0097b387ef52d69f9f9))
* add documentation skeleton and ADR-0001 ([#2](https://github.com/agoranomos/agoranomos/issues/2)) ([0a5e391](https://github.com/agoranomos/agoranomos/commit/0a5e391dedede94c5890f39e47fd1d443d18bd47))
* close out Phase 0 after the final review ([#6](https://github.com/agoranomos/agoranomos/issues/6)) ([db8bf1f](https://github.com/agoranomos/agoranomos/commit/db8bf1f9156e74035283b2b69b455c9a8fcf063a))
* correct the statements on AI access, the Metabase setup and the audience ([#16](https://github.com/agoranomos/agoranomos/issues/16)) ([885dccc](https://github.com/agoranomos/agoranomos/commit/885dcccfb85238a22d0fd3c58e0386801736f61b))
* deploy on a server, and back up and restore ([#57](https://github.com/agoranomos/agoranomos/issues/57)) ([b91b602](https://github.com/agoranomos/agoranomos/commit/b91b602bccf8a65c7a1000d2cee19a21adf1a4ff))
* describe the end-to-end demo as built ([#25](https://github.com/agoranomos/agoranomos/issues/25)) ([f1a3b1f](https://github.com/agoranomos/agoranomos/commit/f1a3b1f12e49e5094e10c6ef35f1d2050b7107e8))
* indent nested lists by four spaces, and check it in a hook ([#43](https://github.com/agoranomos/agoranomos/issues/43)) ([e6122c7](https://github.com/agoranomos/agoranomos/commit/e6122c7b7ea3d4ca84943cfa9436ce4cedae80d5))
* propose ADR-0017 and ADR-0018, levels of AI access and the content of the package in Metabase ([#44](https://github.com/agoranomos/agoranomos/issues/44)) ([44d0aae](https://github.com/agoranomos/agoranomos/commit/44d0aaefef00c65e7c974415a5f801cfe801917b))
* show the demo dashboard in the colors of the project, in light and dark ([#47](https://github.com/agoranomos/agoranomos/issues/47)) ([e569cff](https://github.com/agoranomos/agoranomos/commit/e569cffebebd3126384eab4591d46aab20262588))
* start Phase 1 with the follow-ups of the Phase 0 review ([#10](https://github.com/agoranomos/agoranomos/issues/10)) ([8d6040f](https://github.com/agoranomos/agoranomos/commit/8d6040f701b6c99c4fcad9540b764ab206c1371f))
