---
name: Bug report
about: Something in KiviDB does not work as it should
title: ""
labels: bug
assignees: ""
---

<!--
Security vulnerabilities: do NOT report them here. See SECURITY.md for how to report one privately.
-->

**What happened**

A clear description of the problem.

**How to reproduce**

The commands, client code or configuration that make it happen, step by step:

1.
2.
3.

**What you expected**

What should have happened instead. If Redis behaves differently, say how.

**Environment**

- KiviDB version (`kividb --version`):
- Build variant (`default`, `lua`, `tls` or `full`):
- Installed from (binary, .deb/.rpm, Docker image, built from source):
- OS and architecture:
- I/O model, if on Linux (`--io-model`, or the `I/O model` line in the startup log):
- Client library and version, if relevant:

**Logs**

Relevant lines from the server log (with passwords and private data removed).

**Anything else**

Other context, such as whether it happens every time or only under load.
