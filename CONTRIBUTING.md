# Contributing to slackslaw

Thanks for your interest in contributing!

## Getting started

1. Fork the repo and clone your fork
2. Make sure `slackslaw doctor` passes on your machine
3. Create a feature branch: `git checkout -b my-feature`
4. Make your changes
5. Test manually against a real or test workspace
6. Commit with a clear message and open a pull request

## Guidelines

- Keep it simple — this is a single bash script by design
- Maintain compatibility with bash 4+ and macOS/Linux
- Use the existing coding style (helper functions, color output, etc.)
- Don't add external dependencies beyond python3 and sqlite3
- Test both `--json` and human-readable output paths

## Reporting bugs

Open an issue with:
- Your OS and bash version (`bash --version`)
- slackdump version (`slackdump version`)
- Steps to reproduce
- Expected vs actual behavior

## Code of conduct

Be kind and constructive. We follow the [Contributor Covenant](https://www.contributor-covenant.org/version/2/1/code_of_conduct/).
