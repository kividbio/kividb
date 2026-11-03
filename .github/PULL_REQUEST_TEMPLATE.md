## What this changes

<!-- What the pull request does, and why. Link the issue it fixes, e.g. "Fixes #123". -->

## How it was tested

<!-- The tests you added or ran, and on which OS. -->

## Checklist

- [ ] I have read and signed the [Contributor License Agreement](https://github.com/kividbio/kividb/blob/master/CLA.md) (on a first pull request, comment: "I have read the CLA Document and I hereby sign the CLA").
- [ ] `cargo fmt --all -- --check` passes.
- [ ] `cargo clippy --all-targets -- -D warnings` passes, with and without `--features lua,tls`.
- [ ] `cargo test` passes, with and without `--features lua,tls`.
- [ ] I added or updated tests for this change.
- [ ] I updated the documentation (`COMMANDS.md`, `docs/`, `--help`) where users see a difference.
