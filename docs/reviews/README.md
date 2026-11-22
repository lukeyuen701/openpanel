# docs/reviews — adversarial review records

One file per adversarial review: what was reviewed, what was found, and what
became of each finding. Written by the **`adversarial-review` skill**
(`.claude/skills/adversarial-review/SKILL.md`), which also holds the method.

## Naming

```
YYYY-MM-DD-NN-<slug>.md
```

- `YYYY-MM-DD` — the day the review ran (absolute date, never "today").
- `NN` — two-digit sequence WITHIN that day, starting at `01`. The next number
  is `ls docs/reviews/<date>-* | wc -l` + 1; never renumber an existing file.
- `<slug>` — the scope in kebab-case: `go-worker-p3`, `pr-584-sdk-clicks`,
  `uncommitted-session-buffer`.

There is deliberately **no hand-maintained index** here: `ls docs/reviews/`
sorts by date and sequence, and a list kept by hand goes stale.

## What a record holds

1. **Header** — date, baseline commit, the exact scope (uncommitted diff /
   commit range / branch / PR), the method (how many reviewers, how split), and
   the gates that were or were NOT run.
2. **Conclusion first** — one paragraph: is anything exploitable or losing
   data, where the real breaks cluster.
3. **Findings with stable IDs** (`F01`, `F02` … in severity order; `T01` … for
   test gaps; `N01` … for docs / nits). Each Medium-or-worse finding states the
   file:line, the defect, a concrete failure scenario, and whether it was
   CONFIRMED by reading the code or is INFERRED.
4. **Status** per finding: `open` · `fixed (<commit>)` · `wontfix (<why>)` ·
   `invalid (<why>)`. Never free text such as "fixed (uncommitted)", which
   nobody updates later. A fix that a later review found incomplete keeps
   `fixed` and adds `; residue: <newer record> <IDs>`, so the ledger points at
   where the remainder is tracked.
5. **Recommended order** of fixes.

## Life after the review

A record is a ledger, not a snapshot: when a finding is fixed, refused or
proven wrong, update ITS status line in place (the commit that fixes it says
`fixes docs/reviews/<file> F03` in its body). Never delete a finding — an
`invalid` one is the record of a false positive and teaches the next review
what not to claim. A follow-up review of the same scope is a NEW file that
links the old one.
