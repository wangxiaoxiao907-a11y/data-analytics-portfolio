/* DATA BASE CREATION  */

DROP TABLE IF EXISTS taxi_trajectories;

CREATE TABLE taxi_trajectories (
    taxi_id INT,
    trajectory_id INT,
    timestamp TIMESTAMP,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION
);
COPY taxi_trajectories(taxi_id, trajectory_id, timestamp, longitude, latitude)
FROM '/Users/wangxiaoxiao/t_drive_cleaned.csv' DELIMITER ',' CSV HEADER;

ALTER TABLE taxi_trajectories
ADD COLUMN geom GEOMETRY(POINT, 4326);

UPDATE taxi_trajectories
SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);

-- Add indexes to optimize spatial and time queries
CREATE INDEX idx_geom_gist
ON taxi_trajectories
USING GIST (geom);

CREATE INDEX idx_timestamp
ON taxi_trajectories (timestamp);


-- Query Task 1:
-- Find the top 5 taxis that appeared most frequently 
-- between 2008-02-01 and 2008-02-03.

/* Test Case 1.1: No Index Applied */
/* Test Case 1.1 with NO_INDEX */
-- Count taxi appearances in time window without indexing

SELECT taxi_id, COUNT(*) AS appearance_count
FROM taxi_trajectories
WHERE timestamp BETWEEN '2008-02-01 00:00:00' AND '2008-02-03 00:00:00'
GROUP BY taxi_id
ORDER BY appearance_count DESC
LIMIT 5;

/* Outputs:
 taxi_id | appearance_count
---------+------------------
  6275   | 14381
  3579   | 10017
  3015   | 8624
  8126   | 7466
  366    | 6924
*/
/* QUERY PLAN:
"Limit  (cost=518809.54..518809.55 rows=5 width=12) (actual time=2500.221..2500.239 rows=5 loops=1)"
"  ->  Sort  (cost=518809.54..518810.04 rows=200 width=12) (actual time=2500.220..2500.238 rows=5 loops=1)"
"        Sort Key: (count(*)) DESC"
"        Sort Method: top-N heapsort  Memory: 25kB"
"        ->  Finalize GroupAggregate  (cost=518299.43..518806.22 rows=200 width=12) (actual time=2471.097..2499.867 rows=9679 loops=1)"
"              Group Key: taxi_id"
"              ->  Gather Merge  (cost=518299.43..518802.22 rows=400 width=12) (actual time=2470.906..2498.690 rows=9799 loops=1)"
"                    Workers Planned: 2"
"                    Workers Launched: 2"
"                    ->  Partial GroupAggregate  (cost=517299.41..517756.03 rows=200 width=12) (actual time=2274.151..2300.558 rows=3266 loops=3)"
"                          Group Key: taxi_id"
"                          ->  Sort  (cost=517299.41..517450.95 rows=60616 width=4) (actual time=2274.131..2288.308 rows=524485 loops=3)"
"                                Sort Key: taxi_id"
"                                Sort Method: external merge  Disk: 6336kB"
"                                Worker 0:  Sort Method: external merge  Disk: 6000kB"
"                                Worker 1:  Sort Method: external merge  Disk: 6184kB"
"                                ->  Parallel Seq Scan on taxi_trajectories  (cost=0.00..512484.25 rows=60616 width=4) (actual time=609.001..2235.712 rows=524485 loops=3)"
"                                      Filter: ((""timestamp"" >= '2008-02-01 00:00:00'::timestamp without time zone) AND (""timestamp"" <= '2008-02-03 00:00:00'::timestamp without time zone))"
"                                      Rows Removed by Filter: 5363176"
"Planning Time: 1.773 ms"
"Execution Time: 2502.171 ms"
*/

-- Query Task 2:
-- Build trajectory lines and find top 3 most similar
-- trajectories to trajectory_id = 100 using Hausdorff distance.

DROP TABLE IF EXISTS trajectory_lines;
CREATE TABLE trajectory_lines AS

SELECT
    trajectory_id,
    taxi_id,
    ST_MakeLine(geom ORDER BY timestamp) AS traj_line
FROM
    taxi_trajectories
GROUP BY
    trajectory_id, taxi_id;
	
CREATE INDEX idx_traj_line_gist
ON trajectory_lines
USING GIST (traj_line);
	
SELECT
    a.trajectory_id AS query_id,
    b.trajectory_id AS similar_id,
    ST_HausdorffDistance(a.traj_line, b.traj_line) AS hausdorff_dist
FROM
    trajectory_lines a, trajectory_lines b
WHERE
    a.trajectory_id = 100 
    AND a.trajectory_id != b.trajectory_id
ORDER BY
    hausdorff_dist ASC
LIMIT 3;


/* Outputs:
query_id | similar_id  ｜hausdorff_dist
---------+----------------------------
100      | 14381       |0.05487736600822093     
100      | 10017       |0.05537403362587694
100      | 8624        |0.05599382555247231
*/
/*QUERY PLAN:
"Limit  (cost=1048212159.53..1048212159.88 rows=3 width=16) (actual time=170414.482..170415.266 rows=3 loops=1)"
"  ->  Gather Merge  (cost=1048212159.53..1057821921.09 rows=83563144 width=16) (actual time=170414.481..170414.671 rows=3 loops=1)"
"        Workers Planned: 1"
"        Workers Launched: 1"
"        ->  Sort  (cost=1048211159.52..1048420067.38 rows=83563144 width=16) (actual time=85209.533..85209.534 rows=2 loops=2)"
"              Sort Key: (st_hausdorffdistance(a.traj_line, b.traj_line))"
"              Sort Method: top-N heapsort  Memory: 25kB"
"              Worker 0:  Sort Method: quicksort  Memory: 25kB"
"              ->  Nested Loop  (cost=0.00..1047131121.55 rows=83563144 width=16) (actual time=60.336..85206.634 rows=5168 loops=2)"
"                    Join Filter: (a.trajectory_id <> b.trajectory_id)"
"                    Rows Removed by Join Filter: 0"
"                    ->  Parallel Seq Scan on trajectory_lines a  (cost=0.00..2650.35 rows=497 width=36) (actual time=2.690..3.024 rows=0 loops=2)"
"                          Filter: (trajectory_id = 100)"
"                          Rows Removed by Filter: 5168"
"                    ->  Seq Scan on trajectory_lines b  (cost=0.00..3097.60 rows=168960 width=36) (actual time=0.003..18.365 rows=10336 loops=1)"
"Planning Time: 2.199 ms"
"Execution Time: 170416.247 ms"
*/

-- Query Task 3:
-- For each trajectory, find the top 3 most similar
-- trajectories using approximate method (ST_Distance)
/* Test Case 3.1: Approximate similarity using ST_Distance */
/* Test Case 3.1 with NO_INDEX, filtered input (<200) */

SELECT *
FROM (
    SELECT 
        a.trajectory_id AS query_id,
        b.trajectory_id AS similar_id,
        ST_Distance(a.traj_line, b.traj_line) AS dist,
        ROW_NUMBER() OVER (
            PARTITION BY a.trajectory_id
            ORDER BY ST_Distance(a.traj_line, b.traj_line)
        ) AS rank
    FROM 
        (SELECT * FROM trajectory_lines WHERE trajectory_id < 200) a
    JOIN 
        (SELECT * FROM trajectory_lines WHERE trajectory_id < 200) b
    ON a.trajectory_id != b.trajectory_id
) AS ranked
WHERE rank <= 3;
/* Outputs:
query_id|	similar_id	|  dist	|rank
----------------------------------------
0		|		199		|	0	|	1
0		|		2		|	0	|	2
0		|		3		|	0	|	3
1		|		199		|	0	|	1
1		|		2		|	0	|	2
1		|		3		|	0	|	3
2		|		199		|	0	|	1
2		|		1		|	0	|	2
2		|		3		|	0	|	3
3		|		199		|	0	|	1
3		|		1		|	0	|	2
3		|		2		|	0	|	3
4		|		199		|	0	|	1
4		|		1		|	0	|	2
4		|		2		|	0	|	3
5		|		199		|	0	|	1
5		|		1		|	0	|	2
5		|		2		|	0	|	3
6		|		199		|	0	|	1
6		|		1		|	0	|	2
6		|		2		|	0	|	3
7		|		199		|	0	|	1
7		|		1		|	0	|	2
7		|		2		|	0	|	3
8		|		199		|	0	|	1
8		|		1		|	0	|	2
8		|		2		|	0	|	3
9		|		199		|	0	|	1
9		|		1		|	0	|	2
9		|		2		|	0	|	3
10		|		199		|	0	|	1
10		|		1		|	0	|	2
10		|		2		|	0	|	3
11		|		199		|	0	|	1
11		|		1		|	0	|	2
11		|		2		|	0	|	3
12		|		199		|	0	|	1
12		|		1		|	0	|	2
12		|		2		|	0	|	3
13		|		199		|	0	|	1
13		|		1		|	0	|	2
13		|		2		|	0	|	3
14		|		199		|	0	|	1
14		|		1		|	0	|	2
14		|		2		|	0	|	3
15		|		199		|	0	|	1
15		|		1		|	0	|	2
15		|		2		|	0	|	3
16		|		199		|	0	|	1
16		|		1		|	0	|	2
16		|		2		|	0	|	3
17		|		199		|	0	|	1
17		|		1		|	0	|	2
17		|		2		|	0	|	3
18		|		151		|	3.44E-06	|	1
18		|		2		|	1.84E-05	|	2
18		|		41		|	3.11E-05	|	3
19		|		199		|	0	|	1
19		|		1		|	0	|	2
19		|		2		|	0	|	3
20		|		199		|	0	|	1
20		|		1		|	0	|	2
20		|		2		|	0	|	3
21		|		199		|	0	|	1
21		|		1		|	0	|	2
21		|		2		|	0	|	3
22		|		199		|	0	|	1
22		|		1		|	0	|	2
22		|		2		|	0	|	3
23		|		199		|	0	|	1
23		|		1		|	0	|	2
23		|		2		|	0	|	3
24		|		199		|	0	|	1
24		|		1		|	0	|	2
24		|		2		|	0	|	3
25		|		199		|	0	|	1
25		|		1		|	0	|	2
25		|		2		|	0	|	3
26		|		199		|	0	|	1
26		|		1		|	0	|	2
26		|		2		|	0	|	3
27		|		199		|	0	|	1
27		|		1		|	0	|	2
27		|		2		|	0	|	3
28		|		199		|	0	|	1
28		|		1		|	0	|	2
28		|		2		|	0	|	3
29		|		199		|	0	|	1
29		|		1		|	0	|	2
29		|		2		|	0	|	3
30		|		199		|	0	|	1
30		|		1		|	0	|	2
30		|		2		|	0	|	3
31		|		199		|	0	|	1
31		|		1		|	0	|	2
31		|		2		|	0	|	3
32		|		199		|	0	|	1
32		|		1		|	0	|	2
32		|		2		|	0	|	3
33		|		199		|	0	|	1
33		|		1		|	0	|	2
33		|		2		|	0	|	3
34		|		199		|	0	|	1
34		|		1		|	0	|	2
34		|		2		|	0	|	3
35		|		199		|	0	|	1
35		|		1		|	0	|	2
35		|		2		|	0	|	3
36		|		199		|	0	|	1
36		|		1		|	0	|	2
36		|		2		|	0	|	3
37		|		199		|	0	|	1
37		|		1		|	0	|	2
37		|		2		|	0	|	3
38		|		199		|	0	|	1
38		|		1		|	0	|	2
38		|		2		|	0	|	3
39		|		199		|	0	|	1
39		|		1		|	0	|	2
39		|		2		|	0	|	3
40		|		199		|	0	|	1
40		|		1		|	0	|	2
40		|		2		|	0	|	3
41		|		199		|	0	|	1
41		|		1		|	0	|	2
41		|		2		|	0	|	3
42		|		199		|	0	|	1
42		|		1		|	0	|	2
42		|		2		|	0	|	3
43		|		199		|	0	|	1
43		|		1		|	0	|	2
43		|		2		|	0	|	3
44		|		199		|	0	|	1
44		|		1		|	0	|	2
44		|		2		|	0	|	3
45		|		199		|	0	|	1
45		|		1		|	0	|	2
45		|		2		|	0	|	3
46		|		199		|	0	|	1
46		|		1		|	0	|	2
46		|		2		|	0	|	3
47		|		199		|	0	|	1
47		|		1		|	0	|	2
47		|		2		|	0	|	3
48		|		199		|	0	|	1
48		|		1		|	0	|	2
48		|		2		|	0	|	3
49		|		199		|	0	|	1
49		|		1		|	0	|	2
49		|		2		|	0	|	3
50		|		199		|	0	|	1
50		|		1		|	0	|	2
50		|		2		|	0	|	3
51		|		199		|	0	|	1
51		|		1		|	0	|	2
51		|		2		|	0	|	3
52		|		199		|	0	|	1
52		|		1		|	0	|	2
52		|		2		|	0	|	3
53		|		199		|	0	|	1
53		|		1		|	0	|	2
53		|		2		|	0	|	3
54		|		199		|	0	|	1
54		|		1		|	0	|	2
54		|		2		|	0	|	3
55		|		199		|	0	|	1
55		|		1		|	0	|	2
55		|		2		|	0	|	3
56		|		199		|	0	|	1
56		|		1		|	0	|	2
56		|		2		|	0	|	3
57		|		199		|	0	|	1
57		|		1		|	0	|	2
57		|		2		|	0	|	3
58		|		199		|	0	|	1
58		|		1		|	0	|	2
58		|		2		|	0	|	3
59		|		199		|	0	|	1
59		|		1		|	0	|	2
59		|		2		|	0	|	3
60		|		199		|	0	|	1
60		|		1		|	0	|	2
60		|		2		|	0	|	3
61		|		199		|	0	|	1
61		|		1		|	0	|	2
61		|		2		|	0	|	3
62		|		199		|	0	|	1
62		|		1		|	0	|	2
62		|		2		|	0	|	3
63		|		199		|	0	|	1
63		|		1		|	0	|	2
63		|		2		|	0	|	3
64		|		199		|	0	|	1
64		|		1		|	0	|	2
64		|		2		|	0	|	3
65		|		199		|	0	|	1
65		|		1		|	0	|	2
65		|		2		|	0	|	3
66		|		199		|	0	|	1
66		|		1		|	0	|	2
66		|		2		|	0	|	3
67		|		199		|	0	|	1
67		|		1		|	0	|	2
67		|		2		|	0	|	3
68		|		199		|	0	|	1
68		|		59		|	0	|	2
68		|		60		|	0	|	3
69		|		199		|	0	|	1
69		|		1		|	0	|	2
69		|		2		|	0	|	3
70		|		199		|	0	|	1
70		|		1		|	0	|	2
70		|		2		|	0	|	3
71		|		199		|	0	|	1
71		|		1		|	0	|	2
71		|		2		|	0	|	3
72		|		199		|	0	|	1
72		|		1		|	0	|	2
72		|		2		|	0	|	3
73		|		199		|	0	|	1
73		|		1		|	0	|	2
73		|		2		|	0	|	3
74		|		199		|	0	|	1
74		|		1		|	0	|	2
74		|		2		|	0	|	3
75		|		199		|	0	|	1
75		|		1		|	0	|	2
75		|		2		|	0	|	3
76		|		199		|	0	|	1
76		|		1		|	0	|	2
76		|		2		|	0	|	3
77		|		199		|	0	|	1
77		|		1		|	0	|	2
77		|		2		|	0	|	3
78		|		199		|	0	|	1
78		|		1		|	0	|	2
78		|		2		|	0	|	3
79		|		199		|	0	|	1
79		|		1		|	0	|	2
79		|		2		|	0	|	3
80		|		199		|	0	|	1
80		|		1		|	0	|	2
80		|		2		|	0	|	3
81		|		199		|	0	|	1
81		|		1		|	0	|	2
81		|		2		|	0	|	3
82		|		199		|	0	|	1
82		|		1		|	0	|	2
82		|		2		|	0	|	3
83		|		199		|	0	|	1
83		|		1		|	0	|	2
83		|		2		|	0	|	3
84		|		199		|	0	|	1
84		|		1		|	0	|	2
84		|		2		|	0	|	3
85		|		199		|	0	|	1
85		|		1		|	0	|	2
85		|		2		|	0	|	3
86		|		199		|	0	|	1
86		|		1		|	0	|	2
86		|		2		|	0	|	3
87		|		199		|	0	|	1
87		|		1		|	0	|	2
87		|		2		|	0	|	3
88		|		199		|	0	|	1
88		|		1		|	0	|	2
88		|		2		|	0	|	3
89		|		199		|	0	|	1
89		|		1		|	0	|	2
89		|		2		|	0	|	3
90		|		199		|	0	|	1
90		|		1		|	0	|	2
90		|		2		|	0	|	3
91		|		199		|	0	|	1
91		|		1		|	0	|	2
91		|		2		|	0	|	3
92		|		199		|	0	|	1
92		|		1		|	0	|	2
92		|		2		|	0	|	3
93		|		199		|	0	|	1
93		|		1		|	0	|	2
93		|		2		|	0	|	3
94		|		199		|	0	|	1
94		|		1		|	0	|	2
94		|		2		|	0	|	3
95		|		199		|	0	|	1
95		|		1		|	0	|	2
95		|		2		|	0	|	3
96		|		199		|	0	|	1
96		|		1		|	0	|	2
96		|		2		|	0	|	3
97		|		90		|	0	|	1
97		|		23		|	0	|	2
97		|		58		|	0	|	3
98		|		199		|	0	|	1
98		|		1		|	0	|	2
98		|		2		|	0	|	3
99		|		199		|	0	|	1
99		|		1		|	0	|	2
99		|		2		|	0	|	3
100		|		199		|	0	|	1
100		|		1		|	0	|	2
100		|		2		|	0	|	3
101		|		199		|	0	|	1
101		|		1		|	0	|	2
101		|		2		|	0	|	3
102		|		199		|	0	|	1
102		|		1		|	0	|	2
102		|		2		|	0	|	3
103		|		199		|	0	|	1
103		|		1		|	0	|	2
103		|		2		|	0	|	3
104		|		199		|	0	|	1
104		|		1		|	0	|	2
104		|		2		|	0	|	3
105		|		199		|	0	|	1
105		|		1		|	0	|	2
105		|		2		|	0	|	3
106		|		199		|	0	|	1
106		|		1		|	0	|	2
106		|		2		|	0	|	3
107		|		199		|	0	|	1
107		|		1		|	0	|	2
107		|		2		|	0	|	3
108		|		199		|	0	|	1
108		|		1		|	0	|	2
108		|		2		|	0	|	3
109		|		199		|	0	|	1
109		|		1		|	0	|	2
109		|		2		|	0	|	3
110		|		199		|	0	|	1
110		|		1		|	0	|	2
110		|		2		|	0	|	3
111		|		199		|	0	|	1
111		|		1		|	0	|	2
111		|		2		|	0	|	3
112		|		199		|	0	|	1
112		|		1		|	0	|	2
112		|		2		|	0	|	3
113		|		199		|	0	|	1
113		|		1		|	0	|	2
113		|		2		|	0	|	3
114		|		199		|	0	|	1
114		|		1		|	0	|	2
114		|		2		|	0	|	3
115		|		199		|	0	|	1
115		|		1		|	0	|	2
115		|		2		|	0	|	3
116		|		199		|	0	|	1
116		|		1		|	0	|	2
116		|		2		|	0	|	3
117		|		187		|	1.00E-05	|	1
117		|		49		|	5.37E-05	|	2
117		|		32		|	5.84E-05	|	3
118		|		199		|	0	|	1
118		|		1		|	0	|	2
118		|		2		|	0	|	3
119		|		199		|	0	|	1
119		|		1		|	0	|	2
119		|		2		|	0	|	3
120		|		199		|	0	|	1
120		|		1		|	0	|	2
120		|		2		|	0	|	3
121		|		199		|	0	|	1
121		|		1		|	0	|	2
121		|		2		|	0	|	3
122		|		199		|	0	|	1
122		|		1		|	0	|	2
122		|		2		|	0	|	3
123		|		199		|	0	|	1
123		|		1		|	0	|	2
123		|		2		|	0	|	3
124		|		199		|	0	|	1
124		|		1		|	0	|	2
124		|		2		|	0	|	3
125		|		199		|	0	|	1
125		|		1		|	0	|	2
125		|		2		|	0	|	3
126		|		199		|	0	|	1
126		|		1		|	0	|	2
126		|		2		|	0	|	3
127		|		199		|	0	|	1
127		|		1		|	0	|	2
127		|		2		|	0	|	3
128		|		199		|	0	|	1
128		|		1		|	0	|	2
128		|		2		|	0	|	3
129		|		199		|	0	|	1
129		|		1		|	0	|	2
129		|		2		|	0	|	3
130		|		199		|	0	|	1
130		|		1		|	0	|	2
130		|		2		|	0	|	3
131		|		199		|	0	|	1
131		|		1		|	0	|	2
131		|		2		|	0	|	3
132		|		199		|	0	|	1
132		|		1		|	0	|	2
132		|		2		|	0	|	3
133		|		199		|	0	|	1
133		|		1		|	0	|	2
133		|		2		|	0	|	3
134		|		199		|	0	|	1
134		|		1		|	0	|	2
134		|		2		|	0	|	3
135		|		199		|	0	|	1
135		|		1		|	0	|	2
135		|		2		|	0	|	3
136		|		199		|	0	|	1
136		|		1		|	0	|	2
136		|		2		|	0	|	3
137		|		199		|	0	|	1
137		|		1		|	0	|	2
137		|		2		|	0	|	3
138		|		199		|	0	|	1
138		|		1		|	0	|	2
138		|		2		|	0	|	3
139		|		199		|	0	|	1
139		|		1		|	0	|	2
139		|		2		|	0	|	3
140		|		199		|	0	|	1
140		|		1		|	0	|	2
140		|		2		|	0	|	3
141		|		199		|	0	|	1
141		|		1		|	0	|	2
141		|		2		|	0	|	3
142		|		199		|	0	|	1
142		|		1		|	0	|	2
142		|		2		|	0	|	3
143		|		199		|	0	|	1
143		|		1		|	0	|	2
143		|		2		|	0	|	3
144		|		199		|	0	|	1
144		|		1		|	0	|	2
144		|		2		|	0	|	3
145		|		199		|	0	|	1
145		|		1		|	0	|	2
145		|		2		|	0	|	3
146		|		199		|	0	|	1
146		|		1		|	0	|	2
146		|		2		|	0	|	3
147		|		199		|	0	|	1
147		|		1		|	0	|	2
147		|		2		|	0	|	3
148		|		199		|	0	|	1
148		|		1		|	0	|	2
148		|		2		|	0	|	3
149		|		199		|	0	|	1
149		|		1		|	0	|	2
149		|		2		|	0	|	3
150		|		199		|	0	|	1
150		|		1		|	0	|	2
150		|		2		|	0	|	3
151		|		199		|	0	|	1
151		|		1		|	0	|	2
151		|		2		|	0	|	3
152		|		199		|	0	|	1
152		|		1		|	0	|	2
152		|		2		|	0	|	3
153		|		199		|	0	|	1
153		|		1		|	0	|	2
153		|		2		|	0	|	3
154		|		199		|	0	|	1
154		|		1		|	0	|	2
154		|		2		|	0	|	3
155		|		199		|	0	|	1
155		|		1		|	0	|	2
155		|		2		|	0	|	3
156		|		199		|	0	|	1
156		|		1		|	0	|	2
156		|		2		|	0	|	3
157		|		76		|	0	|	1
157		|		37		|	0	|	2
157		|		36		|	0	|	3
158		|		199		|	0	|	1
158		|		1		|	0	|	2
158		|		2		|	0	|	3
159		|		199		|	0	|	1
159		|		1		|	0	|	2
159		|		2		|	0	|	3
160		|		199		|	0	|	1
160		|		1		|	0	|	2
160		|		2		|	0	|	3
161		|		199		|	0	|	1
161		|		1		|	0	|	2
161		|		2		|	0	|	3
162		|		199		|	0	|	1
162		|		1		|	0	|	2
162		|		2		|	0	|	3
163		|		21		|	0.000679563	|	1
163		|		95		|	0.000944553	|	2
163		|		152		|	0.002029808	|	3
164		|		153		|	0	|	1
164		|		146		|	0	|	2
164		|		7		|	0	|	3
165		|		199		|	0	|	1
165		|		1		|	0	|	2
165		|		2		|	0	|	3
166		|		199		|	0	|	1
166		|		1		|	0	|	2
166		|		2		|	0	|	3
167		|		150		|	0	|	1
167		|		33		|	0	|	2
167		|		35		|	0	|	3
168		|		199		|	0	|	1
168		|		1		|	0	|	2
168		|		2		|	0	|	3
169		|		199		|	0	|	1
169		|		1		|	0	|	2
169		|		2		|	0	|	3
170		|		199		|	0	|	1
170		|		1		|	0	|	2
170		|		2		|	0	|	3
171		|		199		|	0	|	1
171		|		1		|	0	|	2
171		|		2		|	0	|	3
172		|		199		|	0	|	1
172		|		1		|	0	|	2
172		|		2		|	0	|	3
173		|		199		|	0	|	1
173		|		1		|	0	|	2
173		|		2		|	0	|	3
174		|		199		|	0	|	1
174		|		1		|	0	|	2
174		|		2		|	0	|	3
175		|		199		|	0	|	1
175		|		1		|	0	|	2
175		|		2		|	0	|	3
176		|		199		|	0	|	1
176		|		1		|	0	|	2
176		|		2		|	0	|	3
177		|		199		|	0	|	1
177		|		1		|	0	|	2
177		|		2		|	0	|	3
178		|		199		|	0	|	1
178		|		1		|	0	|	2
178		|		2		|	0	|	3
179		|		199		|	0	|	1
179		|		1		|	0	|	2
179		|		2		|	0	|	3
180		|		199		|	0	|	1
180		|		1		|	0	|	2
180		|		2		|	0	|	3
181		|		199		|	0	|	1
181		|		1		|	0	|	2
181		|		2		|	0	|	3
182		|		199		|	0	|	1
182		|		1		|	0	|	2
182		|		2		|	0	|	3
183		|		199		|	0	|	1
183		|		1		|	0	|	2
183		|		2		|	0	|	3
184		|		199		|	0	|	1
184		|		1		|	0	|	2
184		|		2		|	0	|	3
185		|		199		|	0	|	1
185		|		1		|	0	|	2
185		|		2		|	0	|	3
186		|		199		|	0	|	1
186		|		1		|	0	|	2
186		|		2		|	0	|	3
187		|		199		|	0	|	1
187		|		1		|	0	|	2
187		|		2		|	0	|	3
188		|		199		|	0	|	1
188		|		1		|	0	|	2
188		|		2		|	0	|	3
189		|		199		|	0	|	1
189		|		1		|	0	|	2
189		|		2		|	0	|	3
190		|		199		|	0	|	1
190		|		1		|	0	|	2
190		|		2		|	0	|	3
191		|		199		|	0	|	1
191		|		1		|	0	|	2
191		|		2		|	0	|	3
192		|		199		|	0	|	1
192		|		1		|	0	|	2
192		|		2		|	0	|	3
193		|		199		|	0	|	1
193		|		1		|	0	|	2
193		|		2		|	0	|	3
194		|		199		|	0	|	1
194		|		1		|	0	|	2
194		|		2		|	0	|	3
195		|		199		|	0	|	1
195		|		1		|	0	|	2
195		|		2		|	0	|	3
196		|		199		|	0	|	1
196		|		1		|	0	|	2
196		|		2		|	0	|	3
197		|		199		|	0	|	1
197		|		1		|	0	|	2
197		|		2		|	0	|	3
198		|		199		|	0	|	1
198		|		1		|	0	|	2
198		|		2		|	0	|	3
199		|		198		|	0	|	1
199		|		1		|	0	|	2
199		|		2		|	0	|	3

/*query plan
"WindowAgg  (cost=5692.16..1141281.04 rows=39996 width=24) (actual time=172.477..20735.473 rows=600 loops=1)"
"  Run Condition: (row_number() OVER (?) <= 3)"
"  ->  Incremental Sort  (cost=5676.27..640631.11 rows=39996 width=16) (actual time=172.471..20734.208 rows=39800 loops=1)"
"        Sort Key: trajectory_lines.trajectory_id, (st_distance(trajectory_lines.traj_line, trajectory_lines_1.traj_line))"
"        Presorted Key: trajectory_lines.trajectory_id"
"        Full-sort Groups: 200  Sort Method: quicksort  Average Memory: 27kB  Peak Memory: 27kB"
"        Pre-sorted Groups: 200  Sort Method: quicksort  Average Memory: 32kB  Peak Memory: 32kB"
"        ->  Nested Loop  (cost=2488.07..638598.57 rows=39996 width=16) (actual time=14.211..20728.622 rows=39800 loops=1)"
"              Join Filter: (trajectory_lines.trajectory_id <> trajectory_lines_1.trajectory_id)"
"              Rows Removed by Join Filter: 200"
"              ->  Gather Merge  (cost=2488.07..2510.87 rows=200 width=27377) (actual time=12.893..13.150 rows=200 loops=1)"
"                    Workers Planned: 1"
"                    Workers Launched: 1"
"                    ->  Sort  (cost=1488.06..1488.36 rows=118 width=27377) (actual time=3.396..3.440 rows=100 loops=2)"
"                          Sort Key: trajectory_lines.trajectory_id"
"                          Sort Method: quicksort  Memory: 166kB"
"                          Worker 0:  Sort Method: quicksort  Memory: 25kB"
"                          ->  Parallel Seq Scan on trajectory_lines  (cost=0.00..1484.00 rows=118 width=27377) (actual time=0.002..3.335 rows=100 loops=2)"
"                                Filter: (trajectory_id < 200)"
"                                Rows Removed by Filter: 5068"
"              ->  Materialize  (cost=0.00..2208.20 rows=200 width=27377) (actual time=0.000..0.015 rows=200 loops=200)"
"                    ->  Seq Scan on trajectory_lines trajectory_lines_1  (cost=0.00..1537.20 rows=200 width=27377) (actual time=0.010..0.837 rows=200 loops=1)"
"                          Filter: (trajectory_id < 200)"
"                          Rows Removed by Filter: 10136"
"Planning Time: 1.458 ms"
"Execution Time: 20775.481 ms"
*/