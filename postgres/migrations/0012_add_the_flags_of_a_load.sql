-- The flags of the answers of a load (ADR-0054, C8b of #228): four fields of ResponseMetaData of
-- each answer of runReport of the GA4 Data API say that its numbers may not be the whole truth, and
-- the load of GA4 writes them, for each property and load, into four boolean columns of its journal
-- raw_ga4._agoranomos_loads, each true when any answer of the property in the load had it, as the
-- demo of web analytics does into that of raw_demo_web. The pin copies them into core.loads under
-- the same names, from a journal that has them. They are nullable with no default: a row of a
-- source whose answers carry no such flag, an ad platform or a source of the rates, has none, as
-- does a failed account, which got no answer, and every row that a pin wrote before this
-- migration, as nothing recorded the flags then, so that ADD COLUMN rewrites no row. They need no
-- grant of their own: loader may read them, and transformer read and change them, as every other
-- column of the table.

ALTER TABLE core.loads ADD COLUMN is_subject_to_thresholding boolean;
ALTER TABLE core.loads ADD COLUMN has_data_loss_from_other_row boolean;
ALTER TABLE core.loads ADD COLUMN is_sampled boolean;
ALTER TABLE core.loads ADD COLUMN is_truncated boolean;

COMMENT ON TABLE core.loads IS
'The journal of loads: one row for each load of a source and each account in it, which a run of the transform pinned before it read the load, and which it marks accepted once the core holds the window of the account. A failed account has a row with no window. source, load_id and account_id are its key, and load_sk is the reference of the facts. kind tells the usual window of an account from a load of its history, whose rows of the account give the progress of the history, and from a load of its current day. The four flags of a load of web analytics, of GA4 or of the demo in its shape, say whether an answer to the load of the property may not hold the whole truth.';
COMMENT ON COLUMN core.loads.is_subject_to_thresholding IS
'Whether an answer of the GA4 Data API to the load of the property was subject to thresholding, subjectToThresholding of its ResponseMetaData: the report returns only the data that meets the minimum aggregation thresholds, which does not mean that rows were removed. None for a source whose answers carry no such flag, and for a failed account or a row that a pin wrote before the column existed.';
COMMENT ON COLUMN core.loads.has_data_loss_from_other_row IS
'Whether an answer of the GA4 Data API to the load of the property rolled some combinations of its dimensions into the row "(other)", dataLossFromOtherRow of its ResponseMetaData. None for a source whose answers carry no such flag, and for a failed account or a row that a pin wrote before the column existed.';
COMMENT ON COLUMN core.loads.is_sampled IS
'Whether an answer of the GA4 Data API to the load of the property was sampled, samplingMetadatas of its ResponseMetaData set: its numbers are estimated from a sample of the events. None for a source whose answers carry no such flag, and for a failed account or a row that a pin wrote before the column existed.';
COMMENT ON COLUMN core.loads.is_truncated IS
'Whether an answer of the GA4 Data API to the load of the property had its data truncated, dataTruncationReasons of its ResponseMetaData set. None for a source whose answers carry no such flag, and for a failed account or a row that a pin wrote before the column existed.';
