# Security policy

<!-- KIVIDB PRIVATE LIMITED: before publishing, confirm that security@kividb.io exists and is
     monitored, and replace the response-time placeholders below. -->

## Reporting a vulnerability

Please report security vulnerabilities **privately**. Do **not** open a public GitHub issue,
pull request or discussion for a vulnerability, and do not disclose it publicly until a fix
has been released.

Report it by either of these:

- email **security@kividb.io**; or
- a private report through GitHub:
  [Report a vulnerability](https://github.com/kividbio/kividb/security/advisories/new).

Please include:

- the KiviDB version (`kividb --version`) and build variant (`default`, `lua`, `tls` or
  `full`), and how you installed it (binary, package, Docker image, built from source);
- the operating system and architecture;
- the relevant configuration (flags or config file, with passwords removed);
- the steps or commands that reproduce the problem, and what an attacker gains;
- whether the issue is already known to anyone else.

## What happens next

- We acknowledge your report within **[RESPONSE TIME, e.g. 3 business days]**.
- We aim to confirm or rule out the issue, and share our plan for a fix, within
  **[ASSESSMENT TIME, e.g. 10 business days]**.
- We keep you informed of progress, agree a disclosure date with you, and credit you in
  the release notes unless you prefer otherwise.

## Supported versions

Security fixes are made for the latest release line only.

| Version | Security fixes |
|---------|----------------|
| 1.1.x   | Yes            |
| < 1.1   | No             |

Upgrade to the latest 1.1.x release to receive fixes.

## Deployment advice

KiviDB, like Redis, is meant to run on a trusted network. In particular:

- keep [protected mode](README.md#protected-mode) on, or set a password (`--requirepass` or
  ACL users) before accepting clients from other machines;
- do not expose the server port, or the cluster bus port (`<port> + 10000`), to the internet;
- use a TLS build (`tls` or `full`) with `--tls-port` when traffic crosses an untrusted
  network.
