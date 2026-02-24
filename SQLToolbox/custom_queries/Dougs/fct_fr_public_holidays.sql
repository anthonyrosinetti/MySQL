WITH 
-- Calcul de Pâques avec l'algorithme Meeus/Jones/Butcher basé sur cette source: https://en.wikipedia.org/wiki/Computus#Anonymous_Gregorian_algorithm
params AS (
  SELECT
    Y,
    MOD(Y, 19) AS a,
    CAST(FLOOR(Y / 100) AS INT64) AS b,
    MOD(Y, 100) AS c
  FROM UNNEST(GENERATE_ARRAY(2015, 2030)) AS Y
),
step1 AS (
  SELECT
    Y, a, b, c,
    CAST(FLOOR(b / 4) AS INT64) AS d,
    MOD(b, 4) AS e,
    CAST(FLOOR((b + 8) / 25) AS INT64) AS f
  FROM params
),
step2 AS (
  SELECT
    Y, a, b, c, d, e, f,
    CAST(FLOOR((b - f + 1) / 3) AS INT64) AS g
  FROM step1
),
step3 AS (
  SELECT
    Y, a, b, c, d, e, f, g,
    MOD(19 * a + b - d - g + 15, 30) AS h,
    CAST(FLOOR(c / 4) AS INT64) AS i,
    MOD(c, 4) AS k
  FROM step2
),
step4 AS (
  SELECT
    Y, a, h, i, k, e,
    MOD(32 + 2 * e + 2 * i - h - k, 7) AS l
  FROM step3
),
step5 AS (
  SELECT
    Y, a, h, l,
    CAST(FLOOR((a + 11 * h + 22 * l) / 451) AS INT64) AS m
  FROM step4
),
easter_dates AS (
  SELECT
    Y AS year,
    DATE(
      Y,
      CAST(FLOOR((h + l - 7 * m + 114) / 31) AS INT64),
      MOD(h + l - 7 * m + 114, 31) + 1
    ) AS easter_date
  FROM step5
),

-- Jours fériés fixes
fixed_holidays AS (
  SELECT DATE(year, 1, 1) AS holiday_date, 'Jour de l An' AS holiday_name, 'fixed' AS holiday_type
  FROM UNNEST(GENERATE_ARRAY(2015, 2030)) AS year
  UNION ALL
  SELECT DATE(year, 5, 1), 'Fête du Travail', 'fixed' FROM UNNEST(GENERATE_ARRAY(2015, 2030)) AS year
  UNION ALL
  SELECT DATE(year, 5, 8), 'Victoire 1945', 'fixed' FROM UNNEST(GENERATE_ARRAY(2015, 2030)) AS year
  UNION ALL
  SELECT DATE(year, 7, 14), 'Fête Nationale', 'fixed' FROM UNNEST(GENERATE_ARRAY(2015, 2030)) AS year
  UNION ALL
  SELECT DATE(year, 8, 15), 'Assomption', 'fixed' FROM UNNEST(GENERATE_ARRAY(2015, 2030)) AS year
  UNION ALL
  SELECT DATE(year, 11, 1), 'Toussaint', 'fixed' FROM UNNEST(GENERATE_ARRAY(2015, 2030)) AS year
  UNION ALL
  SELECT DATE(year, 11, 11), 'Armistice 1918', 'fixed' FROM UNNEST(GENERATE_ARRAY(2015, 2030)) AS year
  UNION ALL
  SELECT DATE(year, 12, 25), 'Noël', 'fixed' FROM UNNEST(GENERATE_ARRAY(2015, 2030)) AS year
),

-- Jours fériés mobiles (calculés à partir de Pâques)
movable_holidays AS (
  SELECT DATE_ADD(easter_date, INTERVAL 1 DAY) AS holiday_date, 'Lundi de Pâques' AS holiday_name, 'movable' AS holiday_type FROM easter_dates
  UNION ALL
  SELECT DATE_ADD(easter_date, INTERVAL 39 DAY), 'Ascension', 'movable' FROM easter_dates
  UNION ALL
  SELECT DATE_ADD(easter_date, INTERVAL 50 DAY), 'Lundi de Pentecôte', 'movable' FROM easter_dates
)

-- Union de tous les jours fériés
SELECT * FROM fixed_holidays
UNION ALL
SELECT * FROM movable_holidays
