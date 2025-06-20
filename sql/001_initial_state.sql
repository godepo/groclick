CREATE TABLE groclick (
    id UUID,
    clicks INTEGER
) ENGINE MergeTree() ORDER BY id;