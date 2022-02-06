--Find unique no. of users at different levels of subsription
SELECT level, count(distinct(user_id))
 from users
group by level; 

--Find unique no. of users at different levels of subsription and gender demographics
SELECT level, gender, count(distinct(user_id))
 from users
group by level, gender 
order by level, gender;

--Get the song and title with maximum duration time by year
select s.year, song_id, title 
from songs s
join
( SELECT year, max(duration) as m
 from songs
 group by year
) as max_dur
on
s.year = max_dur.year and
s.duration = max_dur.m
order by year;