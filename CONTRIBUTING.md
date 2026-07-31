# Contributing to Harry

Thank you for your interest in contributing to Harry - Performance Scraper for
Oracle Database. Contributions may include bug reports, feature proposals,
documentation improvements, testing, and code.

Harry is developed by Jorge Holgado and commercially supported
under the OneClickDBA brand.

## Opening issues

For bugs or enhancement requests, please open a GitHub issue unless the problem
is security-related.

When reporting a bug, include enough information to reproduce it whenever
possible, such as the Harry version or commit, relevant configuration, expected
behaviour, actual behaviour, and sanitized logs.

Do not report security vulnerabilities in a public issue. Instead, follow the
instructions in our [security policy](./SECURITY.md).

## Contributor License Agreement

Before any code or other copyrightable contribution can be accepted, the
contributor must complete the project's applicable Contributor License
Agreement (CLA).

The CLA preserves a clear chain of ownership and gives Jorge Holgado the rights
necessary to maintain, distribute, sublicense, relicense, and transfer the
project while allowing contributors to retain ownership of their contributions.

By submitting a contribution, you represent that:

- You have the legal right to submit the contribution.
- The contribution does not violate any employment agreement, confidentiality
  obligation, or third-party intellectual-property right.
- If the contribution is made on behalf of, or is owned by, an employer or
  another organization, that organization has authorized the contribution and
  has completed the applicable corporate CLA.

A pull request may be opened before the CLA is completed, but it will not be
merged until the applicable individual or corporate CLA has been accepted by
Jorge Holgado.

Instructions for completing the CLA will be provided in the pull request. The
CLA itself is a legal document separate from this contribution guide.

## Contributing code

All commits must include a Developer Certificate of Origin sign-off using the
contributor's real name and an email address attributable to them:

```text
Signed-off-by: Your Name <you@example.org>
```

You can add this line automatically by committing with `--signoff` or `-s`:

```text
git commit --signoff
```

The sign-off certifies that you have the right to submit the contribution under
the terms described in the [Developer Certificate of Origin][DCO].

The sign-off does not replace the CLA. Both requirements must be satisfied
before a contribution can be merged.

## Pull request process

1. Search the existing issues and pull requests to avoid duplicating work.
2. For a non-trivial change, open an issue first so the proposal can be
   discussed before implementation.
3. Fork the repository and create a focused branch for the change.
4. Keep each pull request limited to one logical change.
5. Add or update tests and documentation where applicable. Public
   documentation changes may also require an update to the separate
   [documentation repository][DOCS].
6. Ensure that your changes do not introduce source code, data, credentials,
   confidential material, or other content that you are not authorized to
   contribute.
7. Submit the pull request with a clear description of what changed, why it
   changed, and how reviewers can validate it. Reference the related issue.
8. Address review feedback and ensure that all required checks pass.

Acceptance of a contribution is at the discretion of the Harry maintainers.
Submitting a pull request does not guarantee that it will be merged.

## Licensing

Accepted contributions become part of Harry and are distributed under the
project's applicable open-source license, currently the Universal Permissive
License, Version 1.0, subject to the CLA.

Third-party code must not be copied into the repository unless its license is
compatible with Harry, all required notices are preserved, and the inclusion
has been approved by a maintainer.

`THIRD_PARTY_LICENSES.txt` is generated and must not be edited manually. After
changing Go dependencies or imports, install the pinned generator and refresh
the notice:

```bash
go install github.com/google/go-licenses/v2@v2.0.1
make licenses
make licenses-check
```

Edit `thirdparty/NOTICE_HEADER.txt` for project-specific attribution changes and
`thirdparty/licenses.tpl` for generated inventory formatting, then regenerate
the notice. Commit the source changes and generated file together.

The software license does not grant permission to use the Harry name, logo,
mascot, visual identity, or associated branding. See
[TRADEMARKS.md](./TRADEMARKS.md).

## Code of conduct

Be respectful and constructive. Follow the
[Contributor Covenant Code of Conduct][COC].

## Questions

If you are unsure whether a proposed contribution is appropriate, open an issue
before investing substantial time in it.

[COC]: https://www.contributor-covenant.org/version/2/1/code_of_conduct/
[DCO]: https://developercertificate.org/
[DOCS]: https://github.com/OneClickDBA/harry-performance-scraper-web
