CREATE TABLE tbl1 (
  name char(32),
  surname char(32),
  description text,
  color char(32),
  extra_info_json json,
  extra_info_jsonb jsonb,
  extra_info_charvar character varying,
  weight smallint
);
INSERT INTO tbl1 (
  name, surname, description, color,
  extra_info_json, extra_info_jsonb,
  extra_info_charvar, weight
)
VALUES
  (
    'Salena', 'Kristi', 'Lorem ipsum dolor sit amet',
    '#ff00ff', '{ "age": 30, "city": "New York" }',
    '{"height": 170, "weight": 60}',
    'Random data 1', '70'
  );
INSERT INTO tbl1 (
  name, surname, description, color,
  extra_info_json, extra_info_jsonb,
  extra_info_charvar, weight
)
VALUES
  (
    'Beckham', 'Jazlyn', 'consectetur adipiscing elit, sed do eiusmod tempor',
    '#fd0100', '{ "age": 25, "city": "London" }',
    '{"height": 175, "weight": 65}',
    'Random data 2', '240'
  );
INSERT INTO tbl1 (
  name, surname, description, color,
  extra_info_json, extra_info_jsonb,
  extra_info_charvar, weight
)
VALUES
  (
    'Sookie', 'Rastus', 'incididunt ut labore et dolore magna aliqua.',
    '#aa0150', '{ "age": 32, "city": "Paris" }',
    '{"height": 180, "weight": 70}',
    'Random data 3', '80'
  );
INSERT INTO tbl1 (
  name, surname, description, color,
  extra_info_json, extra_info_jsonb,
  extra_info_charvar, weight
)
VALUES
  (
    'Jimi', 'Matt', 'Ut enim ad minim veniam, quis nostrud exercitation',
    '#ad0dff', '{ "age": 28, "city": "Berlin" }',
    '{"height": 160, "weight": 55}',
    'Random data 4', '77'
  );
COMMENT ON COLUMN tbl1.name IS ':nosens';
COMMENT ON COLUMN tbl1.surname IS 'some descr :sens';
COMMENT ON COLUMN tbl1.description IS ':sens';
COMMENT ON COLUMN tbl1.extra_info_json IS ':sens';
COMMENT ON COLUMN tbl1.extra_info_jsonb IS ':sens';
COMMENT ON COLUMN tbl1.extra_info_charvar IS ':sens some comment';
