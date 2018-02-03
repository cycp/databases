DROP VIEW IF EXISTS q0, q1i, q1ii, q1iii, q1iv, q2i, q2ii, q2iii, q3i, q3ii, q3iii, q4i, q4ii, q4iii, q4iv, q4v;

-- Question 0
CREATE VIEW q0(era) 
AS
  SELECT MAX(era)
  FROM pitching
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM master
  WHERE weight > 300
;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM master
  WHERE namefirst LIKE '% %'
;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count)
AS
  SELECT birthyear, AVG(height), COUNT(*)
  FROM master
  GROUP BY birthyear
  ORDER BY birthyear
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count)
AS
  SELECT birthyear, AVG(height), COUNT(*)
  FROM master
  GROUP BY birthyear
  HAVING AVG(height)>70
  ORDER BY birthyear
;

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid)
AS
  SELECT m.namefirst, m.namelast, m.playerid, h.yearid
  FROM master m, halloffame h
  WHERE m.playerid = h.playerid AND h.inducted = 'Y'
  ORDER BY yearid DESC

;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid)
AS
  SELECT m.namefirst, m.namelast, m.playerid, h.yearid
  FROM master m, halloffame h, schools s, collegeplaying c
  WHERE m.playerid = h.playerid AND m.playerid = c.playerid AND h.inducted = 'Y' AND c.schoolid = s.schoolid AND s.schoolstate = 'CA'
  ORDER BY h.yearid DESC, s.schoolid ASC, m.playerid ASC
;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
AS
  SELECT m.playerid, m.namefirst, m.namelast, s.schoolid
  FROM (SELECT DISTINCT playerid FROM master) m(playerid), schools s, halloffame h
  WHERE m.playerid = h.playerid AND h.inducted = 'Y'
  ORDER BY m.playerid DESC, s.schoolid ASC
;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
  SELECT b.playerid, m.namefirst, m.namelast, b.yearid, 
    (s.one + 2*s.two + 3*s.three + 4*s.hr) * 1.0 / s.ab as slg
  FROM batting b, master m, 
    (SELECT h-h2b-h3b-hr as one, 2*h2b as two, 3*h3b as three, 
    4*hr as hr, ab FROM batting) s
  WHERE b.ab > 50 and m.playerid = b.playerid
  ORDER BY slg DESC, yearid, playerid
  LIMIT 10
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
  SELECT playerid, namefirst, namelast, 
    (sums.h - sums.h2b - sums.h3b - sums.hr + 2*sums.h2b + 3*sums.h3b + 
      4*sums.hr)*1.0/sums.ab as lslg
  FROM batting, 
    (SELECT SUM(h) h, SUM(h2b) h2b, SUM(h3b) h3b, SUM(hr) hr, SUM(ab) ab FROM batting
      GROUP BY playerid) as sums
  WHERE sums.ab > 50 and b.playerid = sums.playerid
  GROUP BY playerid
  ORDER BY lslg DESC, playerid
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg)
AS
  SELECT 1, 1, 1 -- replace this line
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg, stddev)
AS
  SELECT yearid, MIN(salary), MAX(salary), AVG(salary), STDEV(salary)
  FROM salaries 
  GROUP BY yearid
  ORDER BY yearid
;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
AS
  SELECT width_bucket(salary, MIN(salary), MAX(salary)+1,10) - 1 as binid,
   MIN(salary), MAX(salary), COUNT(*)
  WHERE yearid = 2016
  FROM salaries
  GROUP BY binid
;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
  SELECT 1, 1, 1, 1 -- replace this line
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
  SELECT 1, 1, 1, 1, 1 -- replace this line
;
-- Question 4v
CREATE VIEW q4v(team, diffAvg) AS
  SELECT 1, 1 -- replace this line
;

