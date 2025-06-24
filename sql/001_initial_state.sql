CREATE TABLE groclick (
    id UUID,
    clicks INTEGER
) ENGINE MergeTree() ORDER BY id;

CREATE TABLE groclick_base (id UUID,
                                 clicks INTEGER
) ENGINE MergeTree() ORDER BY id;

CREATE MATERIALIZED VIEW groclick_mv TO groclick_base AS
    SELECT * FROM groclick WHERE id NOT IN (SELECT id FROM groclick_base)
;