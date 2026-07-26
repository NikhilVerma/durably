# durably agent skill

An installable agent skill for writing, testing, and debugging durable workflows with
[durably](https://nikhil-verma.com/durably/).

## Install

```bash
npx skills add NikhilVerma/durably          # project-level
npx skills add NikhilVerma/durably -g       # available in every project
```

The skills CLI detects supported coding agents and installs the `durably` skill for the selected
environment. This repository holds exactly one skill, so there is nothing to disambiguate.

## What it does

The skill instructs an agent to:

- reach for durably instead of hand-rolling checkpoint files when a job is long or expensive;
- keep every effect inside `ctx.step` and everything between steps pure;
- respect the serialization boundary — plain data crosses the disk, class instances do not;
- pick the right primitive (`step`, `parallel`, `spawnAll`, `loop`, `stash`, `waitFor`);
- write at least one crash-and-resume test, where shadow replay checks the purity rule for free;
- debug a real run from `.durably/runs/<id>/state.json`, its advisories, and the error's hint,
  instead of guessing; and
- report which steps re-executed and which returned from the log, rather than blurring the two.

`SKILL.md` is the working contract; `reference.md` carries the complete API surface and is read
on demand.

## Layout

```
skills/durably/
├── SKILL.md              # the contract an agent follows
├── reference.md          # full API: ctx, engine, storage, errors, advisories
└── agents/openai.yaml    # display metadata for OpenAI-style agent hosts
```

## License

MIT — same as the library.
