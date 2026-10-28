# Workflow Rules

## Iterations

Work follows `docs/plan.md`. An iteration has: goal, deliverables, acceptance
criteria, evidence. Do not start the next iteration with a red suite. If scope
changes, edit the plan **first** (docs are the contract), then the code.

## Commits

* One concern per commit. Format: `type(scope): summary`
  * types: `feat`, `fix`, `test`, `docs`, `chore`, `perf`, `refactor`
  * scopes: `gateway`, `broker`, `storage`, `orders`, `worker`, `projector`,
    `kpilot`, `mesh`, `deploy`, `ci`, `docs`
* Subject in imperative mood, <= 72 chars. Body explains *why* and any trade-off.
* A commit must leave the tree runnable: `python -m compileall -q libs services`
  and the relevant test layer green.
* Never commit `runtime/`, logs, databases or secrets. Evidence generated into
  `docs/results/` **is** committed.
* Do not amend or force-push pushed commits.

## Branching

Work happens on the feature branch; the default branch is never pushed to
directly. Push after every meaningful commit so work survives sandbox teardown.

## Bug handling

1. Reproduce with a minimal test (or a script) - capture the failure output.
2. Fix the cause, not the symptom; add a regression test referencing the case.
3. Note the root cause in the commit body ("why") - useful for reviewers.

## Definition of done (checklist)

- [ ] Behaviour implemented behind documented env vars
- [ ] Probes/metrics/log fields exist for new failure modes
- [ ] Test layer updated and green
- [ ] `docs/plan.md` status + README updated if user-visible
- [ ] Focused commit pushed
