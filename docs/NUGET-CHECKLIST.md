## 📋 Release Checklist

Before publishing a release:

* Verify the root `README.md` and package readmes describe only shipped behavior.
* Run `dotnet restore`, `dotnet build --configuration Release`, and `dotnet test --configuration Release` from the repo root.
* Confirm package versions and NuGet metadata are intentional for `Streamix` and `Streamix.Extensions`.
* Keep roadmap items and MVP scope clearly separated so deferred work is not presented as available behavior.

---

## Status

- Streamix is still in an early stage. The root repository README is the authoritative product contract and may describe work that is still being completed.
- Streamix is still early-stage, but this package README is intended to describe the shipped core surface only.
Use the root repository README for the fuller product contract, roadmap, and release checklist.
