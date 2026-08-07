-- A parent can explicitly decline Florence's optional Google setup prompt.
-- Re-engagement may refresh an expired link only while this remains null.

ALTER TABLE people
  ADD COLUMN google_activation_suppressed_at timestamptz;
