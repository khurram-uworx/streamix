# EF Stream Follow-Up

## Purpose

This file collects EF integration backlog items that have been pushed out of earlier release plans.

Settled public contract and current guidance now live in:

- `README.md`
- `GETTING-STARTED.md`
- `ARCHITECTURE.md`
- `src/Streamix.Extensions/README.md`

When this packet is prioritized for a release, the sections below describe a sensible implementation order and the main design questions to resolve. Items not taken for that release stay here as future plans.

## Future Follow-Up Candidates

- provider-specific validation beyond the current documented caveats
- real-world feedback on streamed EF query usage
- re-evaluate EF-specific batching or paging helpers only if existing streamed mode plus core Streamix operators prove insufficient
- re-evaluate caller-owned context support only if a concrete scenario justifies the added API and lifetime complexity
