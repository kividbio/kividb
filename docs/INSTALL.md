# Installing KiviDB

KiviDB is one binary. This page covers the package repositories for Linux; the
[README](../README.md#running-kividb) covers Docker images, release tarballs,
Homebrew and building from source.

- [Debian and Ubuntu (apt)](#debian-and-ubuntu-apt)
- [Fedora, RHEL and compatibles, Amazon Linux 2023 (dnf)](#fedora-rhel-and-compatibles-amazon-linux-2023-dnf)
- [What the package installs](#what-the-package-installs)
- [Upgrading, pinning and going back](#upgrading-pinning-and-going-back)
- [Removing](#removing)
- [The signing key](#the-signing-key)
- [Maintainers: how the repositories are published](#maintainers-how-the-repositories-are-published)

## Package repositories

`https://packages.kividb.io` has an apt repository and a dnf repository with
every stable release from 1.2.0 on, for `amd64`/`x86_64` and `arm64`/`aarch64`.
Release candidates are never in them. Each release is added when it is
published, so `apt upgrade` and `dnf upgrade` keep KiviDB current.

The packages are the default build, without Lua or TLS (see
[Builds](../README.md#builds)). For `EVAL` or a TLS listener, use the `-lua`,
`-tls` or `-full` tarball or image.

They need glibc 2.34 or newer: Debian 12, Ubuntu 22.04, RHEL, Rocky Linux and
AlmaLinux 9, Fedora 35, Amazon Linux 2023, or later. On an older system the
package manager refuses them.

### Debian and Ubuntu (apt)

```bash
sudo apt update && sudo apt install -y ca-certificates curl
sudo install -d -m 0755 /etc/apt/keyrings
curl -fsSL https://packages.kividb.io/kividb.gpg | sudo tee /etc/apt/keyrings/kividb.gpg > /dev/null
sudo tee /etc/apt/sources.list.d/kividb.sources > /dev/null <<'EOF'
Types: deb
URIs: https://packages.kividb.io/deb
Suites: stable
Components: main
Signed-By: /etc/apt/keyrings/kividb.gpg
EOF
sudo apt update && sudo apt install -y kividb
```

The same source as a one-line `.list` file, for tools that only read those:

```bash
echo "deb [signed-by=/etc/apt/keyrings/kividb.gpg] https://packages.kividb.io/deb stable main" \
  | sudo tee /etc/apt/sources.list.d/kividb.list > /dev/null
```

Use one or the other, not both. Both files can also be downloaded:
`https://packages.kividb.io/kividb.sources` and `.../kividb.list`.

### Fedora, RHEL and compatibles, Amazon Linux 2023 (dnf)

```bash
sudo curl -fsSL -o /etc/yum.repos.d/kividb.repo https://packages.kividb.io/kividb.repo
sudo dnf install -y kividb
```

`kividb.repo` is:

```ini
[kividb]
name=KiviDB
baseurl=https://packages.kividb.io/rpm/$basearch
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=https://packages.kividb.io/kividb.asc
```

`gpgcheck=1` checks the signature on each package and `repo_gpgcheck=1` the
signature on the repository's metadata. The first install asks to import the
key: check that its fingerprint is the one under
[The signing key](#the-signing-key) before answering yes (`-y` answers for
you).

### What the package installs

- `/usr/bin/kividb`;
- a systemd unit, `kividb.service`, which runs the server as the `kividb`
  system user, with the unit's sandboxing (`ProtectSystem=strict` and the
  rest; see `packaging/kividb.service`);
- `/etc/kividb/kividb.conf`, which listens on `127.0.0.1:6380` only and turns
  the [startup update check](UPDATE_CHECK.md) off, since the package manager
  does upgrades. It is a configuration file: an upgrade keeps your changes;
- `/var/lib/kividb`, where the snapshot, the AOF and `nodes.conf` live.

Installing enables and starts the unit:

```bash
systemctl status kividb
redis-cli -p 6380 PING
journalctl -u kividb
```

To accept clients from other machines, set a password (`requirepass`) and
change `bind` in `/etc/kividb/kividb.conf`, then `sudo systemctl restart kividb`.
Without systemd (a container, for example), nothing is started; run
`kividb` yourself, as the `kividb` user, from `/var/lib/kividb`.

### Upgrading, pinning and going back

An upgrade restarts the unit if it is running, and leaves it stopped or
disabled if you stopped or disabled it.

```bash
sudo apt update && sudo apt upgrade            # Debian, Ubuntu
sudo dnf upgrade kividb                        # Fedora, RHEL, Amazon Linux
```

The newest 20 releases are kept in the repositories, so you can install a
given one, or go back to it:

```bash
apt list -a kividb                                   # the versions on offer
sudo apt install --allow-downgrades kividb=1.2.0     # that version
sudo apt-mark hold kividb                            # and stay on it

dnf list --showduplicates kividb
sudo dnf install kividb-1.2.0                        # or: sudo dnf downgrade kividb
sudo dnf versionlock add kividb                      # needs the versionlock plugin
```

Before going back across a minor release, read that release's upgrade notes
in [CHANGELOG.md](../CHANGELOG.md): a newer server may have written files an
older one cannot read.

### Removing

```bash
sudo apt remove kividb       # or: sudo dnf remove kividb
```

This stops and disables the unit and removes the binary and the unit. The
`kividb` user, `/var/lib/kividb` (your data) and, with apt, the configuration
are left in place; `sudo apt purge kividb` removes the configuration too. Delete
`/var/lib/kividb` yourself if you no longer want the data.

### The signing key

The apt metadata (`InRelease`, `Release.gpg`), the dnf metadata
(`repomd.xml.asc`) and every RPM are signed with one OpenPGP key, published at
`https://packages.kividb.io/kividb.asc` (and, for apt, `kividb.gpg`). Its
fingerprint is listed on <https://packages.kividb.io>. To see the
fingerprint of the key you downloaded:

```bash
gpg --show-keys /etc/apt/keyrings/kividb.gpg
curl -fsSL https://packages.kividb.io/kividb.asc | gpg --show-keys
```

The `.deb` and `.rpm` files attached to each GitHub Release carry signed build
provenance instead (`gh attestation verify <file> --repo kividbio/kividb`).
The RPMs in the repository are the same builds with the key's signature
added, so their checksums differ from the release's files.

## Maintainers: how the repositories are published

The repositories are the GitHub Pages site of
[`kividbio/packages`](https://github.com/kividbio/packages), served at
`packages.kividb.io`. After a stable release is published, the release
workflow's `package-repos` job (`.github/workflows/package-repos.yml`):

1. downloads the release's four packages and checks them against
   `SHA256SUMS.txt` and their build attestations;
2. checks out the site, adds the packages, signs each RPM, keeps the newest 20
   versions of each package and architecture, regenerates and signs the apt
   (`apt-ftparchive`) and dnf (`createrepo_c`) metadata, all with
   `packaging/repos/build-repos.sh`;
3. pushes the site to `main` as a single commit, replacing the previous one,
   so old packages do not pile up in the git history.

A published version is never replaced: re-running a release adds nothing, and
a different file under a version already published stops the job.

**Size.** A release adds about 25 MB. GitHub Pages sites are limited to 1 GB,
and the job fails above 900 MB; 20 releases are about 500 MB. If more
packages are added per release (for example `-lua`, `-tls` and `-full`
packages, about four times as much), lower `KEEP` in the workflow.

**Secrets.** The job is skipped, with a notice, until these are set on
`kividbio/kividb`:

| Name | Kind | What |
|------|------|------|
| `PACKAGES_GPG_PRIVATE_KEY` | secret | the signing key, ASCII-armored (`gpg --armor --export-secret-keys`) |
| `PACKAGES_GPG_PASSPHRASE` | secret | its passphrase; leave unset for a key without one |
| `PACKAGES_DEPLOY_KEY` | secret | the private half of an SSH deploy key with write access to `kividbio/packages` |
| `PACKAGES_GPG_FINGERPRINT` | variable | optional: the job refuses any other key |

**A dry run.** *Actions → Package repositories → Run workflow*, with a tag
that is already released and `dry_run` ticked, builds the repositories into
an artifact (`package-repos-<tag>`) from the published site and that
release's packages, and publishes nothing. Without the signing key it signs
with a throwaway key. To try the artifact, serve it
(`python3 -m http.server 8000` in its directory) and follow the steps above
with `http://<host>:8000` in place of `https://packages.kividb.io`.

**By hand.** The script runs anywhere with `apt-utils`, `createrepo-c`, `rpm`
and `gnupg`:

```bash
GPG_KEY=<fingerprint> bash packaging/repos/build-repos.sh <site-dir> <dir-with-debs-and-rpms>
```
