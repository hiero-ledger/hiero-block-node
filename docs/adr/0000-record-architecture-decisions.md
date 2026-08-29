# 1. Record architecture decisions

Date: 2026-05-22

## Status

Accepted

## Context

The most impactful problem in the Hiero BN program at scale is knowledge siloing and
how knowledge gets shared across personnel in a team or across teams in an open source manner.
This affects a large part of the day-to-day workload accuracy and impacts features teams are
responsible for: how do I lower my MTTR if I don't know the thing
is intended to work, why it was designed that way, or if any of the assumptions
behind that are still true or not. Making intelligent decisions about what code should
do requires accurate second order thinking to consider the consequences of a decision:

"And then what?".

If I don't know the assumptions and limitations and previous decisions about the code,
how can I possibly understand the implications of the decision I'm making?

These are very real problems to scale an open source team - onboarding community contributors
who do not have these very important contexts because they weren't around when the
conversation happened.

We need to write it down, have it be lightweight, and be reviewable.

We need to record the architectural decisions we make.

We considered notion for visibility, but prefered to have this live alongside
code. This is what tooling supports and is where our review process occurs. We
felt it was important to have "one spot" for these types of things.

We considered the implications of making this open sourced, but considered them
unlikely, and in the event they occured we could rewrite history.

We decided to put this in a pull request to demonstrate how the process should go.

## Decision

We will use Architecture Decision Records, as [described by Michael Nygard](http://thinkrelevance.com/blog/2011/11/15/documenting-architecture-decisions).

We will have these live in the Hiero block-node repo.

We will strive to continue to do these for decisions that we make, and to consider using them as a starting point for discussion.

## Consequences

See Michael Nygard's article, linked above. For a lightweight ADR toolset, see Nat Pryce's [adr-tools](https://github.com/npryce/adr-tools):

```
brew install adr-tools
```

You may use adr-tools as a convenience to either create a new ADR template with a good filename
and number:

```bash
adr new Implement as Unix shell scripts
```

Or (advanced) supercede a previous ADR

```bash
adr new -s 9 Use Rust for performance-critical functionality
```
