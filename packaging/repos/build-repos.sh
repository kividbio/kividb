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
# each package and architecture are kept (GitHub Pages sites are meant to
# stay under 1 GB; a release adds about 25 MB).
#
# Environment:
#   GPG_KEY   fingerprint of the signing key, already in gpg's keyring, usable
#             with no passphrase prompt (the workflow presets the passphrase)
#   KEEP      versions kept per package and architecture (default 20)
#   BASE_URL  where the site is served (default https://packages.kividb.io)
#
# Layout of SITE:
#   deb/dists/stable/{InRelease,Release,Release.gpg}
#   deb/dists/stable/main/binary-{amd64,arm64}/Packages{,.gz}
#   deb/pool/main/k/<package>/<package>_<version>_<arch>.deb
#   rpm/{x86_64,aarch64}/Packages/<package>-<version>-<release>.<arch>.rpm
#   rpm/{x86_64,aarch64}/repodata/ (repomd.xml signed as repomd.xml.asc)
#   kividb.gpg (binary key, for apt), kividb.asc (armored, for dnf)
#   kividb.sources, kividb.list, kividb.repo, index.html, CNAME, .nojekyll
#
# Needs: dpkg-deb, apt-ftparchive (apt-utils), rpm, rpmsign, createrepo_c, gpg.
set -euo pipefail

SITE="$(realpath "${1:?usage: build-repos.sh SITE INCOMING}")"
INCOMING="$(realpath "${2:?usage: build-repos.sh SITE INCOMING}")"
GPG_KEY="${GPG_KEY:?set GPG_KEY to the fingerprint of the signing key}"
KEEP="${KEEP:-20}"
BASE_URL="${BASE_URL:-https://packages.kividb.io}"
BASE_URL="${BASE_URL%/}"

case "$KEEP" in '' | *[!0-9]* | 0) echo "KEEP must be a positive number" >&2; exit 1 ;; esac

DEB_ARCHES="amd64 arm64"
RPM_ARCHES="x86_64 aarch64"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# The key's own ID and its subkeys' IDs, lower case, as rpm prints them: a
# signed RPM must carry one of them.
key_ids="$(gpg --batch --with-colons --list-keys "$GPG_KEY" | awk -F: '$1 == "pub" || $1 == "sub" { print tolower($5) }')"
[ -n "$key_ids" ] || { echo "no key $GPG_KEY in the keyring" >&2; exit 1; }

mkdir -p "$SITE/deb/pool/main" "$SITE/deb/dists/stable/main" "$SITE/rpm"

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
    dest="$SITE/rpm/$arch/Packages/${name}-${ver}-${rel}.${arch}.rpm"
    if [ -e "$dest" ]; then
        # The published copy is signed and the incoming one is not, so the
        # files differ; the payload digest says whether it is the same build.
        a="$(rpm -qp --nosignature --qf '%{PAYLOADDIGEST}' "$rpmf")"
        b="$(rpm -qp --nosignature --qf '%{PAYLOADDIGEST}' "$dest")"
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

# ── Keep the newest $KEEP versions of each package and architecture ──────
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
prune() { # prune deb|rpm FILE... : the same package and architecture
    local kind="$1"
    shift
    [ "$#" -gt "$KEEP" ] || return 0
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
    for f in "${sorted[@]:$KEEP}"; do
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
            prune deb "${same[@]}"
        done
    done
done
for arch in $RPM_ARCHES; do
    d="$SITE/rpm/$arch/Packages"
    [ -d "$d" ] || continue
    mapfile -t rpms < <(find "$d" -maxdepth 1 -type f -name '*.rpm')
    [ "${#rpms[@]}" -gt 0 ] || continue
    for name in $(for f in "${rpms[@]}"; do rpm -qp --nosignature --qf '%{NAME}\n' "$f"; done | sort -u); do
        mapfile -t same < <(for f in "${rpms[@]}"; do
            [ "$(rpm -qp --nosignature --qf '%{NAME}' "$f")" = "$name" ] && printf '%s\n' "$f"; done)
        prune rpm "${same[@]}"
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
rm -f dists/stable/Release dists/stable/InRelease dists/stable/Release.gpg
apt-ftparchive \
    -o APT::FTPArchive::Release::Origin=KiviDB \
    -o APT::FTPArchive::Release::Label=KiviDB \
    -o APT::FTPArchive::Release::Suite=stable \
    -o APT::FTPArchive::Release::Codename=stable \
    -o "APT::FTPArchive::Release::Architectures=$DEB_ARCHES" \
    -o APT::FTPArchive::Release::Components=main \
    -o "APT::FTPArchive::Release::Description=KiviDB packages, $BASE_URL" \
    release dists/stable > "$work/Release"
mv "$work/Release" dists/stable/Release
gpg --batch --yes --local-user "$GPG_KEY" --digest-algo SHA512 --clearsign \
    --output dists/stable/InRelease dists/stable/Release
gpg --batch --yes --local-user "$GPG_KEY" --digest-algo SHA512 --armor --detach-sign \
    --output dists/stable/Release.gpg dists/stable/Release

# ── dnf metadata ─────────────────────────────────────────────────────────
for arch in $RPM_ARCHES; do
    d="$SITE/rpm/$arch"
    mkdir -p "$d/Packages"
    createrepo_c --quiet --general-compress-type=gz --retain-old-md=0 "$d"
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
<p>apt and dnf repositories for <a href="https://github.com/kividbio/kividb">KiviDB</a>, the default
build (no Lua, no TLS), for amd64/x86_64 and arm64/aarch64. Metadata and RPMs are signed with the key
<code>$fpr</code>.</p>
<h2>Debian and Ubuntu</h2>
<pre>sudo install -d -m 0755 /etc/apt/keyrings
curl -fsSL $BASE_URL/kividb.gpg | sudo tee /etc/apt/keyrings/kividb.gpg &gt;/dev/null
curl -fsSL $BASE_URL/kividb.sources | sudo tee /etc/apt/sources.list.d/kividb.sources &gt;/dev/null
sudo apt update &amp;&amp; sudo apt install kividb</pre>
<h2>Fedora, RHEL and compatibles, Amazon Linux 2023</h2>
<pre>sudo curl -fsSL -o /etc/yum.repos.d/kividb.repo $BASE_URL/kividb.repo
sudo dnf install kividb</pre>
<p>Full instructions: <a href="https://github.com/kividbio/kividb/blob/master/docs/INSTALL.md">docs/INSTALL.md</a>.
The newest $KEEP versions are kept.</p>
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
