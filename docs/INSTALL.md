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
- [Maintainers: one-time setup](#maintainers-one-time-setup)

## Package repositories

The apt and dnf repositories at `https://packages.kividb.io` go live with
KiviDB 1.2.1. From then on they have every stable release from 1.2.1 on, for
`amd64`/`x86_64` and `arm64`/`aarch64`. Each release is added when it is
published, so `apt upgrade` and `dnf upgrade` keep KiviDB current. Until then,
and for 1.2.0, use the `.deb` and `.rpm` files attached to each GitHub Release
(see the [README](../README.md#linux-packages)).

The packages are the default build, without Lua or TLS (see
[Builds](../README.md#builds)). For `EVAL` or a TLS listener, use the `-lua`,
`-tls` or `-full` tarball or image.

They need glibc 2.34 or newer: Debian 12, Ubuntu 22.04, RHEL, Rocky Linux and
AlmaLinux 9, Fedora 35, Amazon Linux 2023, or later. On an older system the
package manager refuses them. The `arm64`/`aarch64` packages need an ARMv8.1
CPU or newer, because the binary uses its LSE atomics: Graviton2 and later and
Ampere are fine, a Raspberry Pi 4 is not.

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

Check that the key you downloaded has the fingerprint under
[The signing key](#the-signing-key): `gpg --show-keys /etc/apt/keyrings/kividb.gpg`.

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
sudo apt install --allow-downgrades kividb=1.2.1     # that version
sudo apt-mark hold kividb                            # and stay on it

dnf list --showduplicates kividb
sudo dnf install kividb-1.2.1                        # or: sudo dnf downgrade kividb
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
fingerprint is published here and in [SECURITY.md](../SECURITY.md#package-signing-key),
in this repository, so that you can check the key against a copy that the site
serving it does not control:

**Fingerprint:** `6492 17DE 0F7F D7DB 3D7B  AC3A E21D 8F6E DDE5 F489`
(`KiviDB Packages <packages@kividb.io>`, RSA 4096, created 2026-11-12).

To see the fingerprint of the key you downloaded:

```bash
gpg --show-keys /etc/apt/keyrings/kividb.gpg
curl -fsSL https://packages.kividb.io/kividb.asc | gpg --show-keys
```

If it is not the fingerprint above, do not install from the repository.

The `.deb` and `.rpm` files attached to each GitHub Release carry signed build
provenance instead (`gh attestation verify <file> --repo kividbio/kividb`).
The RPMs in the repository are the same builds with the key's signature
added, so their checksums differ from the release's files.

## Maintainers: how the repositories are published

The repositories are the GitHub Pages site of
[`kividbio/packages`](https://github.com/kividbio/packages), served at
`packages.kividb.io`. After a stable release is published, the release
workflow's `package-repos` job (`.github/workflows/package-repos.yml`), in
the `packages` environment:

1. downloads the release's four packages and checks them against
   `SHA256SUMS.txt` and their build attestations;
2. checks out the site, adds the packages, signs each RPM, keeps the newest 20
   versions of each package and architecture, regenerates and signs the apt
   (`apt-ftparchive`) and dnf (`createrepo_c`) metadata, all with
   `packaging/repos/build-repos.sh`;
3. pushes the site to `main` as a single commit, replacing the previous one,
   so old packages do not pile up in the git history.

A published version is never replaced: re-running a release adds nothing, and
a different file under a version already published stops the job. A package
that does not require glibc 2.34 or newer also stops it, so the packages of
1.2.0 and earlier are never added.

While a new version is published, clients in the middle of an update still
find the indexes they were told about: apt's indexes are also served by their
digest (`by-hash`), and the dnf metadata files are named by theirs. In both,
the previous generation is kept beside the new one and older ones are removed.

**Size.** A release adds about 25 MB. GitHub Pages sites are limited to 1 GB,
and the job fails above 900 MB; 20 releases are about 500 MB. If more
packages are added per release (for example `-lua`, `-tls` and `-full`
packages, about four times as much), lower `KEEP` in the workflow.

**Secrets.** The job is skipped, with a notice, until the key and the deploy
key are set in the `packages` environment of `kividbio/kividb`:

| Name | Kind | What |
|------|------|------|
| `PACKAGES_GPG_PRIVATE_KEY` | secret | the signing key, ASCII-armored (`gpg --armor --export-secret-keys`) |
| `PACKAGES_GPG_PASSPHRASE` | secret | its passphrase; leave unset for a key without one |
| `PACKAGES_DEPLOY_KEY` | secret | the private half of an SSH deploy key with write access to `kividbio/packages` |
| `PACKAGES_GPG_FINGERPRINT` | variable | the key's fingerprint; the job refuses to sign with the real key without it, and with any other key |

**A dry run.** *Actions → Package repositories → Run workflow*, from `master`,
with a tag that is already released and `dry_run` ticked, builds the
repositories into an artifact (`package-repos-<tag>`) from the published site
and that release's packages, and publishes nothing. With the signing key set
up it signs with that key; without it, with a throwaway key. A dry run only
warns about packages without the glibc requirement, so it can be run with
`v1.2.0` before 1.2.1 is out. To try the artifact, serve it
(`python3 -m http.server 8000` in its directory) and follow the steps above
with `http://<host>:8000` in place of `https://packages.kividb.io`.

**By hand.** The script runs anywhere with `apt-utils`, `createrepo-c`, `rpm`
and `gnupg`:

```bash
GPG_KEY=<fingerprint> bash packaging/repos/build-repos.sh <site-dir> <dir-with-debs-and-rpms>
```

## Maintainers: one-time setup

Do these in order, before the release that first publishes the repositories
(1.2.1).

1. **The signing key.** On an offline machine, create an RSA key:

   ```bash
   gpg --quick-gen-key "KiviDB Packages <packages@kividb.io>" rsa4096 sign never
   ```

   It must be RSA. With an ed25519 key, `rpmsign` writes an EdDSA signature
   that is not in the RPM's `RSAHEADER` tag, and `build-repos.sh`'s check that
   every RPM is signed fails. A passphrase is recommended; it goes in its own
   secret (step 7). Keep the private key and a revocation certificate offline
   (`gpg --gen-revoke`), and note the fingerprint
   (`gpg --fingerprint packages@kividb.io`).
2. **Publish the fingerprint.** Replace the placeholder under
   [The signing key](#the-signing-key) and in `SECURITY.md` with it, in a pull
   request to `kividbio/kividb`, so that users can check the key against
   something the site does not serve.
3. **The site's repository.** Create the public repository `kividbio/packages`,
   and give its `main` branch a first commit (a README, say): the workflow
   checks `main` out. Leave `main` unprotected, since every publish
   force-pushes a single new commit to it.
4. **The domain, verified first.** In the organization's settings, *Pages →
   Verified domains*, verify `kividb.io` (a TXT record) before anything points
   at GitHub Pages. Otherwise, while the `CNAME` record exists and no
   repository has claimed the domain, another GitHub user could serve their
   own site at `packages.kividb.io`. Then add the DNS record
   `packages.kividb.io CNAME kividbio.github.io`.
5. **GitHub Pages.** In `kividbio/packages`, *Settings → Pages*: source
   *Deploy from a branch*, `main`, `/ (root)`; custom domain
   `packages.kividb.io`; and, once the certificate is issued, *Enforce HTTPS*.
6. **The deploy key.** Create an SSH key pair for it alone
   (`ssh-keygen -t ed25519 -N '' -C kividb-packages-deploy -f packages-deploy`).
   Add `packages-deploy.pub` to `kividbio/packages` under *Settings → Deploy
   keys*, with *Allow write access*, and the private half,
   `packages-deploy`, as the `PACKAGES_DEPLOY_KEY` secret (step 7). Then
   delete both files.
7. **The `packages` environment.** In `kividbio/kividb`, *Settings →
   Environments*, create `packages` and, under *Deployment branches and tags*,
   allow only the branch `master` and tags matching `v*`. In it, set:
   - the secret `PACKAGES_GPG_PRIVATE_KEY`, from
     `gpg --armor --export-secret-keys <fingerprint>`;
   - the secret `PACKAGES_GPG_PASSPHRASE`, if the key has a passphrase;
   - the variable `PACKAGES_GPG_FINGERPRINT`, the fingerprint, without spaces;
   - the secret `PACKAGES_DEPLOY_KEY`, the private half from step 6.
8. **A dry run with the real key.** Required before the first real publish:
   run the workflow from `master` with `v1.2.0` and `dry_run` ticked (see
   *A dry run*, above). Its log must show the real key's fingerprint, and the
   artifact must install as the steps on this page say, with the key checked
   against the published fingerprint. The 1.2.0 packages are not published:
   they do not require glibc 2.34.
9. **The first publish.** The 1.2.1 release publishes the repositories
   itself. Once it has, check `https://packages.kividb.io` and install from it
   as above.
