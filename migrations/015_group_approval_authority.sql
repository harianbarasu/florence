-- Exact-group approvals are valid only for the conversation and household
-- authority that each person reviewed. Narrower settings or household changes
-- therefore require every current participant to approve again.

ALTER TABLE conversation_rule_approvals
  ADD COLUMN conversation_authority_version bigint,
  ADD COLUMN household_control_epoch bigint;

-- Candidate approvals issued before this authority fence cannot be proven
-- current, so discard them instead of treating them as consent.
DELETE FROM conversation_rule_approvals approval
USING conversation_rules rule
WHERE approval.conversation_rule_id = rule.id
  AND rule.status = 'candidate';

UPDATE conversation_rule_approvals approval
SET conversation_authority_version = conversation.authority_version,
    household_control_epoch = household.control_epoch
FROM conversation_rules rule
JOIN conversations conversation ON conversation.id = rule.conversation_id
JOIN households household ON household.id = conversation.household_id
WHERE approval.conversation_rule_id = rule.id;

ALTER TABLE conversation_rule_approvals
  ADD CONSTRAINT conversation_rule_approvals_authority_version_check
    CHECK (conversation_authority_version IS NULL OR conversation_authority_version > 0),
  ADD CONSTRAINT conversation_rule_approvals_household_control_epoch_check
    CHECK (household_control_epoch IS NULL OR household_control_epoch > 0);
