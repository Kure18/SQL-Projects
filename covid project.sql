##https://www.kaggle.com/datasets/dhruvildave/covid19-deaths-dataset
###1.Cleaning the data by removing duplicate rows

select *
from all_weekly_excess_deaths;

create table all_weekly_excess_deaths2
like all_weekly_excess_deaths;

insert into all_weekly_excess_deaths2
select * 
from all_weekly_excess_deaths;

select *
from all_weekly_excess_deaths2;

select * ,
row_number() over(partition by country,region,region_code,start_date,end_date,days,year,week,population,total_deaths,covid_deaths,expected_deaths,excess_deaths,non_covid_deaths,covid_deaths_per_100k,excess_deaths_per_100k,excess_deaths_pct_change) as row_num
from all_weekly_excess_deaths2;


with duplicate_cte as
(
select * ,
row_number() over(partition by country,region,region_code,start_date,end_date,days,year,week,population,total_deaths,covid_deaths,expected_deaths,excess_deaths,non_covid_deaths,covid_deaths_per_100k,excess_deaths_per_100k,excess_deaths_pct_change) as row_num
from all_weekly_excess_deaths2
)
select *
from duplicate_cte
where row_num>1;

###Note:There was no duplicate row found

###2.Standardizing the data

select *
from all_weekly_excess_deaths2;

select distinct(country)
from all_weekly_excess_deaths2;

select distinct(region)
from all_weekly_excess_deaths2;

select distinct(region_code)
from all_weekly_excess_deaths2;

###Note:There were no spelling errors or lack of uniform format in the data

###3.Removing NULL/BLANK data

select *
from all_weekly_excess_deaths2;

select *
from all_weekly_excess_deaths2
where covid_deaths_per_100k>0;

select *
from all_weekly_excess_deaths2
where covid_deaths>0;
###4.Removing any column or row
select *
from all_weekly_excess_deaths2
where region_code>0;

alter table all_weekly_excess_deaths2 drop
region_code;

###Phase 2 
### Performing Exploratory Data Analysis(EDA)
select min(total_deaths),max(total_deaths),min(population),max(population),min(expected_deaths),max(expected_deaths),min(non_covid_deaths),max(non_covid_deaths),min(covid_deaths_per_100k),max(covid_deaths_per_100k),min(excess_deaths_per_100k),max(excess_deaths_per_100k),min(excess_deaths_pct_change),max(excess_deaths_pct_change)
from all_weekly_excess_deaths2;

select country,sum(population)
from all_weekly_excess_deaths2
group by country
order by country desc;

select country,sum(total_deaths)
from all_weekly_excess_deaths2
group by country;

select *
from all_weekly_excess_deaths2;



