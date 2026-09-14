# Development checks

Use this reference when changing package declarations or preparing the corresponding code commit/release. [AGENTS.md](AGENTS.md) defines scope and verification policy; [README testing](README.md#testing) lists routine local test targets.

## Elisp package conventions

- Target Emacs 29.1+. Keep public names under `clutch-`, private names under the existing private namespaces, and existing public face names unchanged.
- Use conventional Elisp naming, spaces with Emacs indentation, lexical binding and SPDX license metadata. Keep ordinary code near 80 columns where practical; Markdown/Org paragraphs use one source line.
- File headers use `;;; file.el --- Short description -*- lexical-binding: t; -*-` and a matching footer. Keep the description under 60 characters without repeating the package name or “for Emacs”.
- Only `clutch.el` carries package metadata such as Package-Requires, URL, Version and Author. List direct mandatory dependencies there; optional protocol packages are documented and loaded only for their backend. Retain the existing Assisted-by attribution rather than inventing tool/model involvement.
- Autoload user-facing commands and modes. Internal modes stay private; do not autoload implementation helpers. Give every defcustom a type.
- Public functions, macros, options and variables need docstrings. Use a complete first sentence, name arguments in uppercase and avoid indentation visible in help output.
- Use `require` for runtime dependencies, including cl-lib where used. Do not hide runtime requirements in eval-when-compile. A required module supplies its declarations; use defvar/declare-function only for genuine lazy boundaries.
- Buffer-owned state and hooks are buffer-local. Read-only UI derives from special-mode; editing modes derive from the appropriate editing parent. Package registration is allowed at load time, but enabling modes or changing user editing state is not.
- Use text properties for data annotations, overlays for ephemeral visuals, and cached state for rendering. Completion uses completing-read and standard buffer-local CAPFs with `:exclusive 'no`; object resolution and action semantics stay independent of Transient/Embark presentation.
- Use named functions for long-lived registrations; local callbacks can be lambdas. Use macros only when syntax requires them, avoid capture/repeated evaluation, and declare Edebug/indent behavior on changed macros.
- Follow local idioms for conditionals, destructuring, iteration and function references. Prefer direct expressions over wrappers that merely rename a primitive; do not reformat unrelated code to enforce a style preference.

## Non-live code gate

Run from the repository root:

```sh
./test/run-ci.sh all
```

This runs the main and backend ERT suites, byte compilation, package-lint, checkdoc and architecture checks. Byte compilation and lint/doc checks must have zero warnings. `clutch` is one package: package-lint uses `clutch.el` as its metadata source, not each extracted module as a separate package. The runner initializes package.el for dependency metadata; do not move package headers to appease a misconfigured lint invocation.

During iteration, use the affected target or selector from [README](README.md#testing). The full gate is for code/test commits; documentation-only work uses diff and reference checks instead.

## Dependency and surface checks

For code commits, check the applicable boundaries below. A clean search has no matches (rg exit status 1). Inspect a match against the contract rather than deleting legitimate code blindly. The architecture suite checks module dependencies; these searches also cover external private APIs and public configuration/documentation residue.

External private APIs are not allowed; Clutch's own private symbols remain valid within their subsystem:

```sh
rg -n -P "(?<![A-Za-z0-9-])(mysql|pgsql|mongodb|nerd-icons|tramp-rpc)--[A-Za-z0-9-]+" clutch*.el test/*.el
```

When changing MongoDB integration or its documentation, check that protocol implementation, URI interpretation and obsolete public surfaces have not returned to Clutch:

```sh
rg -n -P "require 'mongodb-(wire|bson|params|auth)|(?<![A-Za-z0-9-])mongodb--[A-Za-z0-9-]+|mongosh" clutch*.el test/*.el
rg -n "clutch-mongodb--.*(uri|url)|url-hexify-string|url-unhex-string" clutch-mongodb.el test/*.el
rg -n "conn-wire|:wire|OP_MSG|wire protocol|MongoDB wire" clutch-mongodb.el
rg -n "OP_MSG|wire compression|BSON wrappers|SASLprep|server selection|load-balanced|serviceId|lsid|endSessions|speculative SCRAM" README.md docs PRD.md
rg -n "mongodb[-_]sql(|[-_]interface)" clutch*.el test/*.el README.md docs
rg -n ":driver +'?mongodb|:driver +mongodb" README.md docs PRD.md
```

The public contract is one `mongodb` backend with an optional `:surface sql-interface`. Clutch uses public mongodb- APIs and opaque params/accessors. Protocol details belong in the mongodb.el repository; user documentation links there. The internal JDBC driver key may remain in implementation/tests, but not in user configuration examples.

## Real database workflows

For changes requiring native live coverage:

```sh
./test/run-ci.sh native-live
```

The runner starts or reuses local containers, preferring Podman on Linux and OrbStack-backed Docker on macOS. Its native baseline covers UI PostgreSQL/MySQL and backend PostgreSQL/MySQL/cross-SQL/MongoDB/Redis. Verify that selected fixtures are disposable before allowing writes; do not point the runner at arbitrary user databases.

JDBC coverage requires explicit artifact selection. Replace the example paths with the intended jar and an isolated runtime containing the required drivers:

```sh
CLUTCH_TEST_JDBC_AGENT_JAR=/absolute/path/to/agent.jar \
CLUTCH_TEST_JDBC_AGENT_DIR=/absolute/path/to/isolated-runtime \
./test/run-ci.sh native-live
```

This adds Oracle, SQL Server and ClickHouse containers plus local DuckDB and their supported UI/backend checks. MongoDB SQL Interface and other service-specific endpoints require separate configuration; a community mongod container does not provide SQL Interface. Report passes and capability skips separately, and identify the tested jar. Do not replace the user's installed runtime to run tests. See [native backend testing](docs/native-backends.md) and [JDBC setup](docs/jdbc-backend.org) for environment details.

## Release and documentation details

- Keep CHANGELOG sections version-based: `## VERSION - Unreleased` until an intentional release. Accumulate related changes without bumping a version for each commit.
- Before 1.0, use patch releases for bug-fix-only changes and minor releases for new backends, substantial features or public configuration/API/backend-contract breaks. A real Breaking Changes section precedes Added; omit empty sections.
- Update README and the relevant guide for changed user-facing behavior. Pure tests, internal cleanup and instruction maintenance need no release note unless they change a documented product contract.
- Export data-path changes require content and encoding regressions. Preserve explicit encoding, Excel guidance, atomic replacement, SQL bounds and copy/export scope; a menu-only wording change does not require a new encoding test.
- For published JDBC jar changes, coordinate version and SHA-256 with the agent repository and verify the published bytes. A local Maven build is not evidence of the published checksum. Treat in-place asset replacement as an exceptional repair and document the tradeoff.
- Keep historical postmortems intact. Record non-obvious decisions, abandoned designs and deferred limitations once in the relevant decision record; ordinary instruction maintenance does not require a separate history file.
