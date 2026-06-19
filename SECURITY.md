# Security Policy

## Scope

This is a personal portfolio project demonstrating real-time streaming analytics
with open-source tooling (Redpanda, RisingWave, ClickHouse, Dagster). It
contains no production data, no real user information, and no credentials. All
event data is synthetically generated and statistically calibrated to public
datasets.

## Reporting a Vulnerability

If you discover a security issue in this repository (for example, a dependency
with a known CVE, an insecure default in the docker-compose topology, or a
pattern that could be harmful if copied into a production context), please open
a GitHub issue at the repository's issue tracker. There is no SLA for fixes
given the non-production nature of the project, but all reports are read and
appreciated.

## Dependency Updates

Dependabot is configured to send weekly pull requests for GitHub Actions and
Python (pip) dependencies. Critical CVEs in direct dependencies will be
addressed in a timely patch.
