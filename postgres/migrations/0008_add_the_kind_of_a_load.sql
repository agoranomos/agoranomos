-- The kind of a load in core.loads (ADR-0038, "The first load"): window for the usual window of an
-- account, which a daily load takes, and history for a load of the days before it, which a source
-- takes in the background, in portions by quarter, or a person with the parameters of a run. The
-- pin copies it from the journal of the load, and a journal without it gives window. A row that a
-- pin wrote before this migration takes window, the first history of the demo too, as nothing
-- recorded the kind then. The column is no right: loader may read it, and transformer read and
-- change it, as every other column of the table.

ALTER TABLE core.loads
  ADD COLUMN kind text NOT NULL DEFAULT 'window' CHECK (kind IN ('window', 'history'));

COMMENT ON TABLE core.loads IS
'The journal of loads: one row for each load of a source and each account in it, which a run of the transform pinned before it read the load, and which it marks accepted once the core holds the window of the account. A failed account has a row with no window. source, load_id and account_id are its key, and load_sk is the reference of the facts. kind tells the usual window of an account from a load of its history, whose rows of the account give the progress of the history.';
COMMENT ON COLUMN core.loads.kind IS
'window for a load of the usual window of the account, history for a load of the days before it, which comes in the background in portions by quarter, from the newest to the oldest, or by hand with the parameters of a run.';
