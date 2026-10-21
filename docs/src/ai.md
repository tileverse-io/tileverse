# AI policy

Contributors and users may use AI tools for project work. To keep the code sound, the project secure and the
community healthy, contributions follow the rules below.

## Code and documentation

These rules hold for each contribution, however it was produced:

1. **Responsibility.** The contributor answers for the contribution: its correctness, design, copyright, license and
   fit for the project.
2. **Understanding.** The contributor understands the design, the implementation and the effects of the contribution.
   Do not submit a contribution if you cannot explain it.
3. **Correctness.** The contributor checks that the contribution is correct, complete, and consistent with the
   architecture and conventions of the project. AI tools make mistakes, and catching them is the job of the
   contributor.
4. **Feasibility.** A contribution stays within what the contributor could have produced alone, with reasonable
   research and discussion. AI tools speed the work up. They do not replace the expertise required by the work.
5. **Maintainability.** The contributor and the other project members must be able to maintain the contribution over
   time. Needless complexity and length, in code or in documentation, are a lasting cost.

## Security reports

AI tools may help find and write up a security report. The reporter checks that the problem is real and reproducible
before sending it. A report includes a working reproduction (code, configuration or a request) against a released
version or `main`, not only a plausible description. If a report turns out to be invented or impossible to verify, it
is closed without further discussion.

## Communication

These guidelines apply to issues, pull requests and their comments. Keep messages short and to the point: few readers
have time for long prose.

- State your point in the first sentence. That is where the reader decides whether to read on.
- Keep a message to about one screen. If it needs more, add a final section with the details. A reader must be able to
  skip it without missing the main points.
- One topic per message.
- Many participants are not native English speakers. Use simple, common words.
- Avoid heavy AI-style formatting (headings, bold key terms, nested bullets), even in short messages. If a message
  reads like [AI slop](https://en.wikipedia.org/wiki/AI_slop), readers discount it before reading its content.

In pull requests:

- Comment on code inline, on the line concerned.
- Use a top-level comment for what does not belong to a line: the overall approach, missing tests, scope.
- Link to code or documentation outside the pull request when you refer to it.

In issues:

- The title names the observed symptom, not your guess at its cause.
- The description gives the expected and the actual behavior. Use a numbered list for the steps only when they happen
  in order.
- Add small data samples, configuration or requests to reproduce the problem on someone else's machine.

---

Adapted from the [GeoServer AI policy](https://geoserver.org/ai).
