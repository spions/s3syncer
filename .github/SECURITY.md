# Security policy

## Reporting a vulnerability

Please **do not** open a public issue for security problems. Use GitHub's
[private vulnerability reporting](https://github.com/spions/s3syncer/security/advisories/new)
for this repository and include:

- a description of the issue and its impact
- steps or a proof of concept to reproduce
- affected version (`s3syncer -version`)

You can expect an acknowledgement within a few days. Fixes are released as patch
versions and credited in the release notes unless you prefer otherwise.

## Supported versions

Only the latest release receives security fixes.

## Deployment notes

- `/api/*` requires `api_token` (otherwise it is reachable from loopback only).
- `/metrics` is served without authentication; do not expose it publicly.
- Client authentication uses AWS Signature V2/V4 with a 15 minute clock-skew window;
  terminate TLS in front of s3syncer (reverse proxy / ingress) when it is reachable over untrusted networks.
- Credentials for backends live in the config file; keep it readable only by the service
  (in Kubernetes the chart stores it in a Secret).
