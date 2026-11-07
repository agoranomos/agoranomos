-- A source that stopped (ADR-0035, ADR-0046): more than half of its pinned accounts are in
-- quarantine, which says that the platform or its connector failed, and none of its accounts
-- enters the core in this run. A warning and not an error, which would stop the other sources too,
-- as one table of the core holds the rows of every source (#188): the filter of int_g1_failures
-- stops the source, and Ops and this warning tell a person.
{{ config(severity='warn') }}
select source, count(distinct account_id) as accounts
from {{ ref('int_g1_failures') }}
where reason = 'source_stopped'
group by source
