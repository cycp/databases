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
  SELECT m.namefirst, m.namelast, m.playerid, s.schoolid, h.yearid
  FROM master m, halloffame h, schools s, collegeplaying c
  WHERE m.playerid = h.playerid AND m.playerid = c.playerid 
    AND h.inducted = 'Y' AND c.schoolid = s.schoolid 
    AND s.schoolstate = 'CA'
  ORDER BY h.yearid DESC, s.schoolid ASC, m.playerid ASC
;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
AS
  SELECT m.playerid, m.namefirst, m.namelast, s.schoolid 
  FROM master m
    JOIN halloffame h
      ON m.playerid = h.playerid AND h.inducted = 'Y'
    LEFT OUTER JOIN collegeplaying c
      ON m.playerid = c.playerid
    LEFT OUTER JOIN schools s 
      ON c.schoolid = s.schoolid
    ORDER BY m.playerid DESC, s.schoolid ASC
;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
  SELECT b.playerid, m.namefirst, m.namelast, b.yearid,
    CAST(((h-h2b-h3b-hr) + 2*h2b + 3*h3b + 4*hr)*1.0/ab as float) as slg
  FROM batting b, master m
  WHERE m.playerid = b.playerid and b.ab > 50
  ORDER BY slg DESC, b.yearid, m.playerid
  LIMIT 10
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
  SELECT b.playerid, m.namefirst, m.namelast, 
    CAST(((SUM(h)-SUM(h2b)-SUM(h3b)-SUM(hr)) + 2*SUM(h2b) +
    3*SUM(h3b) + 4*SUM(hr))*1.0/SUM(ab) as float) as lslg
  FROM batting b, master m
  WHERE m.playerid = b.playerid
  GROUP BY m.playerid, b.playerid, m.namefirst, m.namelast
  HAVING SUM(b.ab) > 50
  ORDER BY lslg DESC, m.playerid
  LIMIT 10
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg)
AS 
  SELECT m.namefirst, m.namelast, 
    CAST((SUM(h)-SUM(h2b)-SUM(h3b)-SUM(hr) + 2*SUM(h2b) +
    3*SUM(h3b) + 4*SUM(hr))*1.0/SUM(ab) as float) as lslg
  FROM batting b, master m
  WHERE m.playerid = b.playerid 
  GROUP BY m.playerid
  HAVING SUM(b.ab) > 50 and (SUM(h)-SUM(h2b)-SUM(h3b)-SUM(hr) + 2*SUM(h2b) + 
    3*SUM(h3b) + 4*SUM(hr))*1.0/SUM(ab) > ANY 
      (SELECT ((SUM(h)-SUM(h2b)-SUM(h3b)-SUM(hr)) + 2*SUM(h2b) + 
        3*SUM(h3b) + 4*SUM(hr))*1.0/SUM(ab) 
      FROM batting
      WHERE playerid = 'mayswi01')
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg, stddev)
AS
  SELECT yearid, MIN(salary), MAX(salary), AVG(salary), STDDEV(salary)
  FROM salaries 
  GROUP BY yearid
  ORDER BY yearid
;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
AS



WITH bounds AS
  (SELECT (MAX(salary) - MIN(salary))/10 as range, MIN(salary), MAX(salary)
    FROM salaries
    WHERE yearid = 2016)

WITH bins AS
  (SELECT width_bucket(salary, bounds.min, bounds.max+1, 10) - 1 as binid
    FROM salaries, bounds
    WHERE yearid = 2016
    GROUP BY salary)


SELECT binid, MIN(salary) + (MAX(salary)-MIN(salary))*binid, 
  MIN(salary) + (MAX(salary)-MIN(salary))*(1+binid), COUNT(*)
FROM bins
WHERE yearid=2016
GROUP BY binid
ORDER BY binid

 











-- SELECT width_bucket(salary, bounds.min, bounds.max+1, 10) - 1 as binid, 
--   MIN(salary), MAX(salary), COUNT(*)
-- FROM salaries,
--   (SELECT MIN(salary), MAX(salary)
--     FROM salaries WHERE yearid=2016) bounds
--   WHERE yearid=2016
--   GROUP BY binid
--   ORDER BY binid


 
 -- SELECT bins.binid,
 --   MIN(salary) + bins.range*bins.binid as low, MIN(salary) + bins.range*(1+bins.binid) as high, 
 --    COUNT(*)
 -- FROM salaries,
 --   (SELECT width_bucket(salary, bounds.min, bounds.max+1, 10) - 1 as binid, bounds.range as range
 --     FROM salaries,
 --      (SELECT MIN(salary), MAX(salary), (MAX(salary) - MIN(salary)) / 10 as range
 --            FROM salaries WHERE yearid=2016) bounds
 --     WHERE yearid=2016) bins
 -- WHERE yearid=2016
 -- GROUP BY binid, bins.range
 -- ORDER BY binid




 -- SELECT bins.binid,
 --   MIN(salary) + bins.range*bins.binid as low, MIN(salary) + bins.range*(1+bins.binid) as high, 
 --    COUNT(*)
 -- FROM salaries,
 --   (SELECT width_bucket(salary, bounds.min, bounds.max+1, 10) - 1 as binid, bounds.range as range
 --     FROM salaries,
 --      (SELECT MIN(salary), MAX(salary), (MAX(salary) - MIN(salary)) / 10 as range
 --            FROM salaries WHERE yearid=2016) bounds
 --     WHERE yearid=2016) bins
 -- WHERE yearid=2016
 -- GROUP BY binid, bins.range
 -- ORDER BY binid




;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
  SELECT curr.yearid, MIN(curr.salary) - MIN(prev.salary), 
    MAX(curr.salary) - MAX(prev.salary), AVG(curr.salary) - AVG(prev.salary)
  FROM salaries prev, salaries curr
  WHERE curr.yearid - 1 = prev.yearid
  GROUP BY curr.yearid
  ORDER BY yearid
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
  SELECT s.playerid, m.namefirst, m.namelast, s.salary, s.yearid
  FROM salaries s, master m
  WHERE s.playerid = m.playerid and ((s.yearid = 2000 and s.salary =
    (SELECT MAX(salary) 
      FROM salaries 
      WHERE yearid = 2000
      GROUP BY yearid))
  or (s.yearid = 2001 and s.salary =
    (SELECT MAX(salary) 
      FROM salaries 
      WHERE yearid = 2001
      GROUP BY yearid)))
;
-- Question 4v
CREATE VIEW q4v(team, diffAvg) AS
  SELECT a.teamid as team, MAX(s.salary) - MIN(s.salary) as diffAvg
  FROM allstarfull a, salaries s
  WHERE a.yearid = s.yearid and s.yearid = 2016 and a.playerid = s.playerid
  GROUP BY team
  ORDER BY team
;

