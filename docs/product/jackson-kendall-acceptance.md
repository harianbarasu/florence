# Jackson and Kendall acceptance contract

Florence is not ready for a two-parent pilot until these flows work from real phones without an
operator, database intervention, magic keywords, or advance explanation. We exercise them in short
iterations rather than waiting for every source integration before learning from the group
coordination experience.

## Iteration A — Direct group coverage

This is the first production exercise because the deployed product already supports the complete
coverage state machine without waiting on Gmail backfill.

### 1. Jackson meets Florence naturally

Jackson sends any ordinary first message to the Florence number, for example:

> Hey, what can you help our family with?

Florence answers plainly, explains that she can retain permitted family context, links privacy
details, and asks for consent in natural language. Jackson may consent with an ordinary affirmative
reply; no `START`, `NAME`, or other command is required. Florence learns what to call him, offers to
create his family, and moves to a secure mobile-web handoff when structured controls are useful.

### 2. Jackson connects Google while setup continues

After the household is resolved, Florence offers Jackson a private, single-use handoff for personal
Google. Sync begins immediately without waiting for Kendall. Florence confirms meaningful private
milestones while Jackson can continue adding children, schools, activities, routines, and a
co-parent. Declining Google does not block Florence or create repeated generic reminders.

### 3. Kendall joins without rediscovering the product

Jackson adds Florence and Kendall to an iMessage group. Florence remains completely silent in that
group while it is observe-only. New post-addition messages belong only to that exact chat epoch; they
do not give Florence write authority or make Kendall a Florence user.

Jackson writes a natural introduction such as “Florence, this is my wife Kendall.” With exactly one
unknown current participant, Florence may treat Jackson's statement only as a relationship proposal;
it does not establish Kendall's identity or consent. Florence privately asks that exact participant
whether she is Kendall and whether she wants to join the family as an equal parent. No other unknown
group participant receives an enrollment message merely because Florence observed the group.

Kendall's private acceptance claims her observed identity and accepts the household relationship in
one clear flow. Florence does not ask her to repeat family facts Jackson already supplied. Kendall
reviews those facts, corrects conflicts, and adds missing information or non-conflicting aliases.
Because every current participant is now a registered member of one family, the exact group becomes
interactive automatically; neither parent completes a second per-chat approval. Florence posts one
brief replay-safe activation acknowledgment. A later participant change revokes current write
authority and recomputes whether the new audience is still an all-household group.

### 4. Florence knows enough family context

At minimum the household can represent:

- each child's preferred name and known aliases;
- birth year or age when supplied;
- school;
- activities; and
- recurring pickup/drop-off facts needed for coverage.

Different names such as Jonathan, John, and Johnny are treated as possible aliases, not immediate
conflicts. Real contradictions are flagged once to the parent stewards.

### 5. The first coverage loop closes naturally

For the first exercise, include the time explicitly:

> **Jackson:** I can't pick Avery up Wednesday at 3:00.
>
> **Florence:** Avery's Wednesday pickup at 3:00 is uncovered. Kendall, can you take it?
>
> **Kendall:** Yeah, I'll get her.
>
> **Florence:** Covered — Kendall has Avery's Wednesday 3:00 pickup.

Pass conditions:

- Florence derives the obligation only from permitted context and current evidence.
- The named proposed holder is the only person who can accept for themselves.
- Natural acknowledgments work; silence, delivery, read state, and historical habits do not.
- A reply to Florence targets the exact loop. If more than one loop matches, Florence asks which one.
- If a consequential fact is missing, Florence asks once and advances the existing provisional loop.
- A private decline reveals only that coverage remains open unless the person approves more detail.
- Reminders occur before the useful boundary, remain neutral, and stop immediately after coverage.

After authorized household context is wired into interpretation, repeat without the time:

> **Jackson:** I can't pick Avery up Wednesday.

Florence must recover the time only from the current authorized routine/family projection. If more
than one current routine matches, she asks one narrow question instead of guessing.

## Iteration B — Google-derived coverage

1. Florence privately confirms connection and reports only meaningful milestones: recent sources
   being reviewed, current information reconciled, older history continuing, or an actionable error.
2. A synthetic school email and a later update or cancellation arrive in the same thread, with a
   matching Calendar revision when appropriate.
3. Florence keeps candidate findings silent until the newest thread, relevant attachment, and current
   Calendar state are reconciled.
4. If the obligation remains current and uncovered, Florence privately offers Jackson the minimum
   operational risk. Raw private email never enters the family group.
5. Jackson approves the exact item from iMessage. Florence opens the minimum-meaning loop in the
   exact interactive household group; Kendall accepts naturally; Florence closes it.
6. Repeat with a later cancellation. Florence suppresses an obsolete prompt or updates the same
   already-open loop; it never creates a duplicate.

## Production gate

Each iteration passes only after its complete flow runs through the production Linq number, Railway
web and worker services, PostgreSQL, the configured model, and two independent browser sessions.
Synthetic checks support that evidence but cannot replace it.
