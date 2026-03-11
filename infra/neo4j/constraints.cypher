CREATE CONSTRAINT paper_id IF NOT EXISTS
FOR (p:Paper) REQUIRE p.id IS UNIQUE;

CREATE CONSTRAINT author_name IF NOT EXISTS
FOR (a:Author) REQUIRE a.name IS UNIQUE;

CREATE INDEX paper_year IF NOT EXISTS
FOR (p:Paper) ON (p.year);

CREATE INDEX paper_venue IF NOT EXISTS
FOR (p:Paper) ON (p.venue);

