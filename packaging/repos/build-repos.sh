#!/bin/bash
# Adds packages to the apt and dnf repositories at packages.kividb.io and
# signs their metadata. Run by .github/workflows/package-repos.yml; it can be
# run by hand the same way, with a throwaway key, to test the repositories.
#
#   build-repos.sh SITE INCOMING
#
# SITE is the repository's tree (the kividbio/packages checkout, or an empty
# directory the first time); INCOMING holds the .deb and .rpm files to add,
# named anything. The packages already in SITE are kept, so users can pin or
# go back to an older version, except that only the newest $KEEP versions of
# kividb and $KEEP_FULL of kividb-full are kept, per architecture (GitHub
# Pages sites are meant to stay under 1 GB; a release adds about 26 MB of
# kividb and 30 MB of kividb-full).
#
# Environment:
#   GPG_KEY   fingerprint of the signing key, already in gpg's keyring, usable
#             with no passphrase prompt (the workflow presets the passphrase)
#   KEEP      versions of each package kept per architecture (default 16)
#   KEEP_FULL the same for kividb-full (default 8)
#   BASE_URL  where the site is served (default https://packages.kividb.io)
#   GLIBC_DEP_CHECK  fail (default) or warn: what to do with a package that
#             does not say it needs glibc 2.34 or newer (docs/INSTALL.md
#             promises that it does; the packages before 1.2.1 do not)
#
# Layout of SITE:
#   deb/dists/stable/{InRelease,Release,Release.gpg}
#   deb/dists/stable/main/binary-{amd64,arm64}/Packages{,.gz}
#   deb/dists/stable/main/binary-{amd64,arm64}/by-hash/<algorithm>/<digest>
#     (this generation's indexes and the previous one's)
#   deb/pool/main/k/<package>/<package>_<version>_<arch>.deb
#   rpm/{x86_64,aarch64}/Packages/<package>-<version>-<release>.<arch>.rpm
#   rpm/{x86_64,aarch64}/repodata/ (repomd.xml signed as repomd.xml.asc;
#     the previous generation's files are kept beside it)
#   kividb.gpg (binary key, for apt), kividb.asc (armored, for dnf)
#   kividb.sources, kividb.list, kividb.repo, index.html, CNAME, .nojekyll
#
# Needs: dpkg-deb, apt-ftparchive (apt-utils), rpm, rpmsign, createrepo_c, gpg.
set -euo pipefail

SITE="$(realpath "${1:?usage: build-repos.sh SITE INCOMING}")"
INCOMING="$(realpath "${2:?usage: build-repos.sh SITE INCOMING}")"
GPG_KEY="${GPG_KEY:?set GPG_KEY to the fingerprint of the signing key}"
KEEP="${KEEP:-16}"
KEEP_FULL="${KEEP_FULL:-8}"
BASE_URL="${BASE_URL:-https://packages.kividb.io}"
BASE_URL="${BASE_URL%/}"

GLIBC_DEP_CHECK="${GLIBC_DEP_CHECK:-fail}"

for n in "$KEEP" "$KEEP_FULL"; do
    case "$n" in '' | *[!0-9]* | 0) echo "KEEP and KEEP_FULL must be positive numbers" >&2; exit 1 ;; esac
done
keep_of() { # keep_of PACKAGE: how many versions of it are kept
    if [ "$1" = kividb-full ]; then echo "$KEEP_FULL"; else echo "$KEEP"; fi
}
case "$GLIBC_DEP_CHECK" in fail | warn) ;; *) echo "GLIBC_DEP_CHECK must be fail or warn" >&2; exit 1 ;; esac

DEB_ARCHES="amd64 arm64"
RPM_ARCHES="x86_64 aarch64"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# The key's own ID and its subkeys' IDs, lower case, as rpm prints them: a
# signed RPM must carry one of them.
key_ids="$(gpg --batch --with-colons --list-keys "$GPG_KEY" | awk -F: '$1 == "pub" || $1 == "sub" { print tolower($5) }')"
[ -n "$key_ids" ] || { echo "no key $GPG_KEY in the keyring" >&2; exit 1; }

mkdir -p "$SITE/deb/pool/main" "$SITE/deb/dists/stable/main" "$SITE/rpm"

# Versions are compared the way apt and dnf compare them (dpkg, and rpm's own
# epoch:version-release comparison), not by file name: sort -V puts
# 1.2.0+1-1 before 1.2.0-1.
newer() { # newer deb|rpm A B: A is a newer version than B
    if [ "$1" = deb ]; then
        dpkg --compare-versions "$2" gt "$3"
    else
        [ "$(rpm --eval "%{lua: print(rpm.ver('$2') > rpm.ver('$3'))}")" = true ]
    fi
}

# The packages promise glibc 2.34 or newer (packaging/nfpm.yaml), so apt and
# dnf refuse an older system instead of installing a server that cannot
# start. A package that does not say so is not added.
glibc_dep_missing() { # glibc_dep_missing FILE WHAT
    if [ "$GLIBC_DEP_CHECK" = warn ]; then
        echo "::warning::$1: $2; added anyway (GLIBC_DEP_CHECK=warn)" >&2
        return 0
    fi
    echo "::error::$1: $2; the repositories only take packages that require glibc 2.34 or newer" >&2
    exit 1
}

# ── Add the incoming packages ────────────────────────────────────────────
# A version that is already published is never replaced: an apt or dnf
# client that cached its checksum would refuse the new file, and a pinned
# version must stay what it was. The same package again (a re-run) is a
# no-op; a different file under the same version is an error.
shopt -s nullglob
added=0
for deb in "$INCOMING"/*.deb; do
    name="$(dpkg-deb -f "$deb" Package)"
    ver="$(dpkg-deb -f "$deb" Version)"
    arch="$(dpkg-deb -f "$deb" Architecture)"
    case " $DEB_ARCHES " in *" $arch "*) ;; *) echo "skip $deb: architecture $arch" >&2; continue ;; esac
    need="$(dpkg-deb -f "$deb" Depends | tr ',' '\n' | sed -n 's/^ *libc6 *(>= *\([^)]*\)).*/\1/p' | head -n 1)"
    if [ -z "$need" ]; then
        glibc_dep_missing "$deb" "no Depends: libc6 (>= ...)"
    elif dpkg --compare-versions "$need" lt 2.34; then
        glibc_dep_missing "$deb" "depends on libc6 (>= $need)"
    fi
    first="$(printf %s "$name" | cut -c1)"
    dest="$SITE/deb/pool/main/$first/$name/${name}_${ver//:/%3a}_${arch}.deb"
    if [ -e "$dest" ]; then
        if cmp -s "$deb" "$dest"; then
            echo "deb: $name $ver $arch is already published"
            continue
        fi
        echo "::error::deb: $name $ver $arch is already published with other contents; a published version is never replaced" >&2
        exit 1
    fi
    mkdir -p "$(dirname "$dest")"
    cp "$deb" "$dest"
    echo "deb: added $name $ver $arch"
    added=$((added + 1))
done

for rpmf in "$INCOMING"/*.rpm; do
    nevra="$(rpm -qp --nosignature --qf '%{NAME} %{VERSION} %{RELEASE} %{ARCH}' "$rpmf")"
    read -r name ver rel arch <<<"$nevra"
    case " $RPM_ARCHES " in *" $arch "*) ;; *) echo "skip $rpmf: architecture $arch" >&2; continue ;; esac
    need="$(rpm -qp --nosignature --requires "$rpmf" | awk '$1 == "glibc" && $2 == ">=" { print $3; exit }')"
    if [ -z "$need" ]; then
        glibc_dep_missing "$rpmf" "no Requires: glibc >= ..."
    elif newer rpm 2.34 "$need"; then
        glibc_dep_missing "$rpmf" "requires glibc >= $need"
    fi
    dest="$SITE/rpm/$arch/Packages/${name}-${ver}-${rel}.${arch}.rpm"
    if [ -e "$dest" ]; then
        # The published copy is signed and the incoming one is not, so the
        # files differ. Signing leaves the header and the payload as they
        # are: their digests say whether it is the same build. Both must be
        # there, or two different builds would both read (none) and match.
        a="$(rpm -qp --nosignature --qf '%{PAYLOADDIGEST} %{SHA256HEADER}' "$rpmf")"
        b="$(rpm -qp --nosignature --qf '%{PAYLOADDIGEST} %{SHA256HEADER}' "$dest")"
        for d in $a $b; do
            case "$d" in
                *[!0-9a-f]* | '')
                    echo "::error::rpm: $name $ver-$rel $arch: no payload or header digest in $rpmf or $dest ($a / $b), so it cannot be told apart from the published one" >&2
                    exit 1
                    ;;
            esac
        done
        [ "$(wc -w <<<"$a $b")" -eq 4 ] || { echo "::error::rpm: $name $ver-$rel $arch: unexpected digests ($a / $b)" >&2; exit 1; }
        if [ "$a" = "$b" ]; then
            echo "rpm: $name $ver-$rel $arch is already published"
            continue
        fi
        echo "::error::rpm: $name $ver-$rel $arch is already published with other contents; a published version is never replaced" >&2
        exit 1
    fi
    mkdir -p "$(dirname "$dest")"
    cp "$rpmf" "$work/sign.rpm"
    # %__gpg: Ubuntu's rpm looks for gpg2, which is not there.
    rpmsign --define "_gpg_name $GPG_KEY" --define "_gpg_digest_algo sha512" \
        --define "__gpg $(command -v gpg)" --addsign "$work/sign.rpm" 2>&1 >/dev/null |
        { grep -v 'Could not set GPG_TTY' || true; } >&2
    # rpm -K checks a signature against rpm's own keyring, which this machine
    # need not have: check that there is one, and that our key made it.
    sig="$(rpm -qp --nosignature --qf '%{RSAHEADER:pgpsig}' "$work/sign.rpm")"
    signed=no
    for id in $key_ids; do
        case "$sig" in *"Key ID $id"*) signed=yes ;; esac
    done
    if [ "$signed" != yes ]; then
        echo "::error::rpm: $rpmf was not signed with $GPG_KEY ($sig)" >&2
        exit 1
    fi
    mv "$work/sign.rpm" "$dest"
    echo "rpm: added and signed $name $ver-$rel $arch"
    added=$((added + 1))
done
echo "added $added package(s)"

# ── Keep the newest versions of each package and architecture ────────────
# $KEEP of kividb, $KEEP_FULL of kividb-full. Compared with newer(), above.
prune() { # prune deb|rpm KEEP FILE... : the same package and architecture
    local kind="$1" keep="$2"
    shift 2
    [ "$#" -gt "$keep" ] || return 0
    # Insertion sort, newest first: a few dozen files at most.
    local sorted=() f v i j vj
    local -A ver=()
    for f in "$@"; do
        if [ "$kind" = deb ]; then
            v="$(dpkg-deb -f "$f" Version)"
        else
            v="$(rpm -qp --nosignature --qf '%{EPOCHNUM}:%{VERSION}-%{RELEASE}' "$f")"
        fi
        ver[$f]="$v"
        i=${#sorted[@]}
        sorted+=("$f")
        while [ "$i" -gt 0 ]; do
            j=$((i - 1))
            vj="${ver[${sorted[$j]}]}"
            newer "$kind" "$v" "$vj" || break
            sorted[i]="${sorted[$j]}"
            sorted[j]="$f"
            i=$j
        done
    done
    for f in "${sorted[@]:$keep}"; do
        echo "pruned ${f#"$SITE"/} (${ver[$f]})"
        rm -f "$f"
    done
}
for pkgdir in "$SITE"/deb/pool/main/*/*/; do
    for arch in $DEB_ARCHES; do
        mapfile -t debs < <(find "$pkgdir" -maxdepth 1 -type f -name "*_${arch}.deb")
        [ "${#debs[@]}" -gt 0 ] || continue
        for name in $(for f in "${debs[@]}"; do dpkg-deb -f "$f" Package; done | sort -u); do
            mapfile -t same < <(for f in "${debs[@]}"; do
                [ "$(dpkg-deb -f "$f" Package)" = "$name" ] && printf '%s\n' "$f"; done)
            prune deb "$(keep_of "$name")" "${same[@]}"
        done
    done
done
for arch in $RPM_ARCHES; do
    d="$SITE/rpm/$arch/Packages"
    [ -d "$d" ] || continue
    mapfile -t rpms < <(find "$d" -maxdepth 1 -type f -name '*.rpm')
    [ "${#rpms[@]}" -gt 0 ] || continue
    # Each file's package name, read once: kividb and kividb-full share the
    # directory, and the files pruned for one are gone for the next.
    declare -A rpm_name=()
    for f in "${rpms[@]}"; do rpm_name[$f]="$(rpm -qp --nosignature --qf '%{NAME}' "$f")"; done
    for name in $(printf '%s\n' "${rpm_name[@]}" | sort -u); do
        mapfile -t same < <(for f in "${rpms[@]}"; do
            [ "${rpm_name[$f]}" = "$name" ] && printf '%s\n' "$f"; done)
        prune rpm "$(keep_of "$name")" "${same[@]}"
    done
done

# ── apt metadata ─────────────────────────────────────────────────────────
cd "$SITE/deb"
for arch in $DEB_ARCHES; do
    out="dists/stable/main/binary-$arch"
    mkdir -p "$out"
    apt-ftparchive --arch "$arch" packages pool/main > "$out/Packages"
    gzip -9nc "$out/Packages" > "$out/Packages.gz"
    cat > "$out/Release" <<EOF
Archive: stable
Component: main
Origin: KiviDB
Label: KiviDB
Architecture: $arch
EOF
done
# by-hash (Acquire-By-Hash): apt fetches each index by its digest, so a
# client that read the previous InRelease still gets the files it names
# while this one is published. The files of the previous generation stay;
# older ones are removed below.
if [ -f dists/stable/Release ]; then
    cp dists/stable/Release "$work/Release.previous"
else
    : > "$work/Release.previous"
fi
rm -f dists/stable/Release dists/stable/InRelease dists/stable/Release.gpg
apt-ftparchive \
    -o APT::FTPArchive::DoByHash=true \
    -o APT::FTPArchive::Release::Origin=KiviDB \
    -o APT::FTPArchive::Release::Label=KiviDB \
    -o APT::FTPArchive::Release::Suite=stable \
    -o APT::FTPArchive::Release::Codename=stable \
    -o "APT::FTPArchive::Release::Architectures=$DEB_ARCHES" \
    -o APT::FTPArchive::Release::Components=main \
    -o "APT::FTPArchive::Release::Description=KiviDB packages, $BASE_URL" \
    release dists/stable > "$work/Release"
mv "$work/Release" dists/stable/Release
grep -q '^Acquire-By-Hash: yes$' dists/stable/Release || { echo "apt-ftparchive wrote no Acquire-By-Hash" >&2; exit 1; }
awk 'NF == 3 && $1 ~ /^[0-9a-f]+$/ { print $1 }' "$work/Release.previous" dists/stable/Release |
    sort -u > "$work/by-hash.keep"
find dists/stable -path '*/by-hash/*' -type f | while IFS= read -r f; do
    grep -qxF "${f##*/}" "$work/by-hash.keep" || { echo "by-hash: removed ${f#dists/stable/}"; rm -f "$f"; }
done
for arch in $DEB_ARCHES; do
    d="dists/stable/main/binary-$arch/by-hash/SHA256"
    h="$(sha256sum "dists/stable/main/binary-$arch/Packages" | cut -d' ' -f1)"
    [ -f "$d/$h" ] || { echo "no by-hash copy of binary-$arch/Packages" >&2; exit 1; }
done
gpg --batch --yes --local-user "$GPG_KEY" --digest-algo SHA512 --clearsign \
    --output dists/stable/InRelease dists/stable/Release
gpg --batch --yes --local-user "$GPG_KEY" --digest-algo SHA512 --armor --detach-sign \
    --output dists/stable/Release.gpg dists/stable/Release

# ── dnf metadata ─────────────────────────────────────────────────────────
for arch in $RPM_ARCHES; do
    d="$SITE/rpm/$arch"
    mkdir -p "$d/Packages"
    # The previous generation's metadata files stay beside the new ones, so
    # a client that read the previous repomd.xml still finds what it names.
    # createrepo_c keeps that one (--retain-old-md=1) but leaves alone the
    # generations before it, which no repomd.xml names any more: those are
    # removed here.
    prev="$work/repomd-$arch.previous"
    if [ -f "$d/repodata/repomd.xml" ]; then
        cp "$d/repodata/repomd.xml" "$prev"
    else
        : > "$prev"
    fi
    createrepo_c --quiet --general-compress-type=gz --retain-old-md=1 "$d"
    cat "$prev" "$d/repodata/repomd.xml" | grep -o 'location href="repodata/[^"]*"' |
        cut -d/ -f2 | tr -d '"' | sort -u > "$work/repodata.keep"
    for f in "$d/repodata"/*; do
        case "${f##*/}" in repomd.xml | repomd.xml.asc) continue ;; esac
        grep -qxF "${f##*/}" "$work/repodata.keep" || { echo "repodata: removed ${f#"$SITE"/}"; rm -f "$f"; }
    done
    gpg --batch --yes --local-user "$GPG_KEY" --digest-algo SHA512 --armor --detach-sign \
        --output "$d/repodata/repomd.xml.asc" "$d/repodata/repomd.xml"
done

# ── Keys, client configuration, landing page ─────────────────────────────
cd "$SITE"
gpg --batch --yes --export "$GPG_KEY" > kividb.gpg
gpg --batch --yes --armor --export "$GPG_KEY" > kividb.asc
fpr="$(gpg --batch --with-colons --fingerprint "$GPG_KEY" | awk -F: '$1 == "fpr" { print $10; exit }')"

cat > kividb.sources <<EOF
Types: deb
URIs: $BASE_URL/deb
Suites: stable
Components: main
Signed-By: /etc/apt/keyrings/kividb.gpg
EOF
echo "deb [signed-by=/etc/apt/keyrings/kividb.gpg] $BASE_URL/deb stable main" > kividb.list
cat > kividb.repo <<EOF
[kividb]
name=KiviDB
baseurl=$BASE_URL/rpm/\$basearch
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=$BASE_URL/kividb.asc
EOF

host="${BASE_URL#*://}"
host="${host%%/*}"
case "$BASE_URL" in https://*) printf '%s\n' "$host" > CNAME ;; esac
touch .nojekyll

{
    cat <<EOF
<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>KiviDB packages</title>
<style>body{font:16px/1.5 system-ui,sans-serif;max-width:46rem;margin:2rem auto;padding:0 1rem}pre{background:#f4f4f4;padding:.75rem;overflow-x:auto}code{font-size:.9em}</style>
</head><body>
<h1>KiviDB packages</h1>
<p>apt and dnf repositories for <a href="https://github.com/kividbio/kividb">KiviDB</a>, for
amd64/x86_64 and arm64/aarch64: <code>kividb</code>, the default build (no Lua, no TLS), and
<code>kividb-full</code>, with Lua scripting and TLS. Install one or the other; installing one replaces
the other. Metadata and RPMs are signed with the key <code>$fpr</code>.</p>
<h2>Debian and Ubuntu</h2>
<pre>sudo install -d -m 0755 /etc/apt/keyrings
curl -fsSL $BASE_URL/kividb.gpg | sudo tee /etc/apt/keyrings/kividb.gpg &gt;/dev/null
curl -fsSL $BASE_URL/kividb.sources | sudo tee /etc/apt/sources.list.d/kividb.sources &gt;/dev/null
sudo apt update &amp;&amp; sudo apt install kividb      # or: kividb-full</pre>
<h2>Fedora, RHEL and compatibles, Amazon Linux 2023</h2>
<pre>sudo curl -fsSL -o /etc/yum.repos.d/kividb.repo $BASE_URL/kividb.repo
sudo dnf install kividb      # or: kividb-full</pre>
<p>Full instructions: <a href="https://github.com/kividbio/kividb/blob/master/docs/INSTALL.md">docs/INSTALL.md</a>.
The newest $KEEP versions of <code>kividb</code> and $KEEP_FULL of <code>kividb-full</code> are kept.</p>
<h2>Versions</h2>
<ul>
EOF
    for f in deb/pool/main/*/*/*.deb rpm/*/Packages/*.rpm; do
        [ -e "$f" ] || continue
        printf '<li><a href="%s">%s</a></li>\n' "$f" "${f##*/}"
    done
    echo '</ul></body></html>'
} > index.html

echo "signing key: $fpr"
du -sh "$SITE" | awk '{ print "site size: " $1 }'
size_kb="$(du -sk "$SITE" --exclude=.git | awk '{ print $1 }')"
if [ "$size_kb" -gt 900000 ]; then
    echo "::error::the site is ${size_kb} KB, over 900 MB: lower KEEP (GitHub Pages sites are limited to 1 GB)" >&2
    exit 1
fi
