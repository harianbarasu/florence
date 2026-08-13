# Jackson and Kendall pilot acceptance

Florence is ready for the private two-parent pilot only when this journey works from two real phones
and two independent browser sessions without database intervention, magic conversational wording, or
operator repair.

## 1. Jackson sets up the family

Jackson signs into his own pilot browser session, creates the family, and enters:

- Jackson and Kendall as adults;
- each child's preferred name and aliases;
- birth year;
- school;
- grade, academic year, and effective date; and
- relevant activities.

The page is resumable and editable after onboarding. Child facts remain structured family context,
not chat transcript. Jackson cannot mark Kendall verified from the browser.

## 2. Each adult connects privately

Jackson issues Kendall's private Linq enrollment code from the dashboard. Kendall sends the entire
code to Florence in a one-to-one iMessage. Pass conditions:

- the code expires, is one-use, and a retry returns the same result;
- only a keyed digest is durable;
- the raw code never appears in household events, model input, or group context;
- the live chat contains exactly Kendall's identity;
- Kendall becomes the planned adult, not a browser-selected identity; and
- Florence queues one private confirmation.

Jackson and Kendall then use separate browser credentials. Neither session can read or mutate the
other person's unrelated household authority.

## 3. The exact family group becomes interactive

Jackson creates a group with Kendall and Florence. The first ordinary message is accepted only when
the live Linq participant set is exactly the two verified adults for exactly one household.

Adding a third person, removing a parent, using an old provider handle, or receiving an ambiguous
provider observation must reject the message and produce no group output. Exact duplicate webhooks
must not duplicate cognition, events, or effects.

## 4. One family episode closes naturally

Use a real but synthetic obligation, for example:

> Jackson: The field trip form is due Friday.

Florence should either ask one necessary clarification or create one source-linked open episode.
Kendall can say naturally that she will handle it. Florence records Kendall as owner only because the
message came from Kendall. At the useful follow-up time Florence sends a neutral reminder. Kendall
then says it is handled, the episode closes, and no stale reminder remains.

Pass conditions:

- no task/project/run object is exposed to the family;
- silence, delivery, read state, or model inference never assigns an owner;
- replay causes no second model call or effect;
- reassignment/update cites a current authorized message and advances the episode version;
- timer authority is rechecked when it fires; and
- reminders are factual and blame-free.

## 5. Jackson connects Google privately

Jackson starts Google OAuth from his own session. Florence binds the connection to Jackson regardless
of browser-supplied data. Tokens remain encrypted outside household events and never enter model
context or logs.

A synthetic school email is observed. Florence privately stages only normalized current meaning for
Jackson. Before explicit promotion:

- Kendall and the group cannot see the candidate;
- no raw email body or attachment enters family context; and
- Gmail IDs alone are durable at ingestion.

Jackson explicitly approves sharing the exact minimum meaning. Only that stored candidate version and
digest enters family context.

## 6. Jackson approves one Calendar event

The same private candidate contains a visible Calendar draft. Jackson explicitly approves that exact
draft in his private conversation. Florence inserts one deterministic event into Jackson's primary
calendar, rereads it, and records a receipt only if summary, times, time zone, status, and approval
markers match.

Timeout and conflict retries must find the same provider event. A mismatched reread fails closed and
does not claim success.

## Production gate

Automated tests support but do not replace the live rehearsal. The release passes only when the full
journey runs through:

- the production Linq number;
- Railway API and worker on the same commit;
- PostgreSQL migration `007` and a fresh worker heartbeat;
- the configured OpenAI model;
- one Google pilot account; and
- two independent adult browser sessions.

Use synthetic family data for the first production rehearsal. A passing local Docker build does not
authorize deployment or production data mutation.
