# The startup update check

When KiviDB starts, it makes one HTTPS request to this repository's GitHub
releases page to learn whether a newer release is out. If one is, the server
prints a short notice, once, above the startup banner.

## What is sent

One `GET` request, over HTTPS, for
`https://github.com/kividbio/kividb/releases/latest` — the page you would
open in a browser to find the newest release. The only header that says
anything about the server is the `User-Agent`, `kividb/<version>`.

Nothing else is sent: no query parameters, host name, IP address,
configuration, key or dataset information, and no identifier. GitHub sees
the source IP address of the request, as any web server would.

## What is read

GitHub answers that page with a redirect to the newest stable release,
for example:

```
HTTP/1.1 302 Found
Location: https://github.com/kividbio/kividb/releases/tag/v1.1.0
```

KiviDB reads the tag from the `Location` header and does not follow the
redirect or read any body. No API, no token, no rate limit. Pre-releases
(`-rc` tags) are never "latest" on GitHub, so a stable build is only ever
told about stable releases; a release candidate is told about the release
that follows it.

Only a version-shaped tag is accepted: an optional `v`, numbers separated by
dots, and an optional `-suffix` of letters, digits and dots, at most 40
characters. Anything else in the reply is ignored and nothing from it is
printed, so the reply cannot put text, escape sequences or anything else
on the operator's terminal.

A repository with no release yet redirects to its releases list instead;
that is taken as nothing to report.

## What is shown

Only when the newest release is newer than the running build:

```
  +--------------------------------------------------------------------------+
  | A new KiviDB release is available: 1.1.0 (you are running 1.0.6).        |
  | Download it from https://github.com/kividbio/kividb/releases/tag/v1.1.0 |
  +--------------------------------------------------------------------------+
```

Versions compare as `major.minor.patch`. A release is newer than its own
release candidates (`1.1.0` > `1.1.0-rc1`), and `rc2` is newer than `rc1`.

## When

Once per server start, before the banner is printed, so that what it finds
appears above it. Startup waits for the reply for at most 1.5 seconds; a
slower reply is dropped, never printed later among the log lines. It never
touches a client connection. A failure (no network, DNS, TLS, a timeout, an
unexpected reply) is logged at debug level only (`--loglevel debug`), and
otherwise ignored.

## Turning it off

Any one of these turns it off:

- `update-check no` in the config file;
- `--update-check no` on the command line;
- `KIVIDB_NO_UPDATE_CHECK=1` in the environment.

It is also never made by a debug build, or when the `CI` environment
variable is set.

`update-check-url` (config file) or `--update-check-url` points the check
somewhere else, for example a mirror inside a closed network. Only
`https://` URLs are accepted, and the address has to answer the way GitHub's
`releases/latest` does: a redirect to `.../releases/tag/<tag>`.
