-- An account in quarantine (ADR-0035): the checks kept its window out of the core in this run. A
-- warning, as a quarantine is the run working as meant: the core keeps the data of the account as
-- it was, and the other accounts go on. The view ops.ops_quarantine says why.
{{ config(severity='warn') }}
select source, account_id, reason, count(*) as failures
from {{ ref('int_g1_failures') }}
group by source, account_id, reason
