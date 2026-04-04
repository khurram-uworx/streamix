Use README.md to understand the STREAMIX, our current repository

I want to make Streams Composable Across Boundaries, and for this we need to Add Creation Operators
I want you to focus on this only onwards; as this can be a critical gap

👉 Without rich sources, adoption stalls, we might need more ways to enter our system.

Today we might only rely on From(IAsyncEnumerable) but real-world systems are:
- event-driven
- callback-based
- polling-based

We should have things like:

Stream.From(Task<T>)
Stream.From(Func<Task<T>>)
Stream.Defer(() => ...)
Stream.Create(async (emitter, ct) => { ... })
Stream.Generate(...)
Stream.Interval(TimeSpan)

[src\Streamix\Stream.cs] and [src\Streamix\Single.cs] has few factory methods, lets create [docs\CREATION.md] file with a plan to cover this gap (if any)
Suggest what else we should have and then we will review and iterate on this plan

---

Understand this repo using README.md

I am going to release the next version of Streamix and docs\CREATION.md is the major feature we are targeting for this release. We added Creation Operators in Streamix

- the current public creation APIs in src/Streamix/Stream.cs and src/Streamix/Single.cs
- the creation-operator implementations in src/Streamix/Implementations/Stream.cs and src/Streamix/Implementations/Single.cs
- the current tests in src/Streamix.Tests
- the public contract described in README.md

We were following the plan as its outlined in the CREATION.md and our previous review discovered gaps that we documented in docs\CREATION-REVIEW.md

I want you to review the current status again, and if you think we have done what was needed, great we can start planning the release, otherwise I want you to create docs\CREATION-TASKS.md with list of tasks that we still need to do with intention that we will feed these tasks to Coding Agents

---

Understand this repo using README.md

I am going to release the next version of Streamix and docs\VARIETY.md is the should-have feature we are targeting for this release. We added Creation Operators and next trying to add variety in Termination and Sink Operators to achieve the excellence at system boundaries.

First I want you to review the current status against VARIETY.md and create docs\VARIETY-REVIEW.md accordingly.

Second I want you to create docs\VARIETY-TASKS.md with list of tasks needed for the release to happen with intent that we feed these tasks to Coding Agent.

Third I want you to create docs\VARIETY-NEXT-TASKS.md with list of tasks that we can feed to coding agent after the release to full fill our whole VARIETY plan.
