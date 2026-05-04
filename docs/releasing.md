# Releasing

DurableFlow publishes manually from GitHub Actions using pre-1.0 SemVer versions such as `0.1.0`, `0.1.1`, and `0.2.0`.

## First Release Setup

Before the first release, configure a pending trusted publisher on RubyGems.org:

- Gem name: `durable_flow`
- Repository owner: `skorfmann`
- Repository name: `durableflow`
- Workflow filename: `release.yml`
- Environment: `release`

## Publish

Open **Actions -> Release gem -> Run workflow**, select `main`, and enter the version to publish.

The workflow validates the `x.y.z` version input, writes that version into `lib/durable_flow/version.rb` inside the CI checkout, runs the test suite, builds the gem, authenticates to RubyGems through trusted publishing, and pushes the gem. No RubyGems API token is stored in GitHub.
