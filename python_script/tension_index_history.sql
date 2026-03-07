CREATE TABLE IF NOT EXISTS tension_score_history (
    id          SERIAL PRIMARY KEY,
    country     TEXT NOT NULL,
    snapshot_at DATE NOT NULL DEFAULT CURRENT_DATE,
    tension_score INTEGER,
    tension_level TEXT,
    UNIQUE (country, snapshot_at)
);

CREATE INDEX idx_tsh_country ON tension_score_history (country);
CREATE INDEX idx_tsh_date    ON tension_score_history (snapshot_at DESC);