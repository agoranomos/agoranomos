---
status: accepted
date: 2026-10-02
decision-makers: Andrei Gromov
---

# Write documentation with Zensical and Diátaxis

## Context and Problem Statement

Documentation is part of the product. People have to be able to run, operate and extend the stack, and to understand why it is built the way it is. The project needs a tool that builds the documentation site, a structure for the content, a policy on languages and a home for the decision records.

Which tool and structure do we use, and in which languages do we write?

## Decision Drivers

* The documentation is Markdown in the repository and changes together with the code, through pull requests.
* A new site should not start on a tool that is close to its end of life.
* The tooling is Python-based, like the rest of the project ([ADR-0012](0012-python-and-tooling.md)).
* Every page should have one clear job, and contributors should know where a new page belongs.
* A broken link or a missing page should fail the build, not reach readers.
* English reaches the widest group of readers, and Russian-speaking users should have the key pages in their language too.

## Considered Options

* Zensical, with the Diátaxis structure
* Material for MkDocs
* Docusaurus
* Sphinx

## Decision Outcome

Chosen option: "Zensical, with the Diátaxis structure", because Material for MkDocs is in maintenance and reaches end of life in November 2026, while Zensical reads MkDocs configuration and keeps the Python and Markdown toolchain.

In practice this means:

* **Tool.** The site is built with Zensical. Its version is pinned in the development dependencies and locked in `uv.lock`, and CI builds the site with `zensical build --strict`, so a broken link, anchor or navigation entry fails the pull request.
* **Structure.** The content follows [Diátaxis](https://diataxis.fr/): tutorials, how-to guides, reference and explanation. Each part has one job.
* **Decision records.** ADRs live in `docs/adr` and are published with the site ([ADR-0001](0001-record-architecture-decisions.md)).
* **Languages.** English is the primary language. The README (as `README.ru.md`) and the key pages are also provided in Russian.
* **Hosting.** The site is planned to be published on GitHub Pages.

The other options were not chosen for these reasons:

* **Material for MkDocs** reaches end of life in November 2026, so a new site should not be built on it.
* **Docusaurus** is built on JavaScript and would add a Node.js toolchain next to the Python tooling.
* **Sphinx** uses reStructuredText as its native markup, and Markdown needs an extension, while the documentation of this project is Markdown.

### Consequences

* Good, because the site is built with a tool that reads MkDocs configuration, so the project does not start on a tool that is reaching its end of life.
* Good, because Diátaxis gives every page one job and every contributor a clear place for new pages.
* Good, because the strict build catches broken links, anchors and navigation entries before merge.
* Good, because English as the primary language reaches the widest group of readers.
* Bad, because Zensical is young (its versions are still 0.0.x). Behavior can change between releases, so the version is pinned and the build is strict, and the content stays plain Markdown that another tool can read.
* Bad, because the Russian README and key pages take effort to maintain and can lag behind the English text, which is the reference when they differ.

## More Information

* Versions as of October 2026: Zensical 0.0.67. Material for MkDocs is at 9.7.7, in maintenance.
* The site is configured in `zensical.toml`. Zensical also shows the `status` of an ADR as a page status, which is why the statuses are defined there.
* Planned: the dbt documentation is generated and published together with the site.
* [Zensical](https://zensical.org/) and [Diátaxis](https://diataxis.fr/) are described on their own sites.
