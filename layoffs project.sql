###links-https://www.kaggle.com/datasets/swaptr/layoffs-2022
###1.Cleaning the data by removing duplicate rows
create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

select *,row_number() over(
partition by industry,total_laid_off,percentage_laid_off,`date`) as row_num
from layoffs_staging;

with duplicate_cte as
(
select *,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num>1;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2;

insert into layoffs_staging2
select *,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised) as row_num
from layoffs_staging;

select *
from layoffs_staging2
where row_num>1;
###2.standardizing the data
select distinct company
from layoffs_staging2;

select distinct industry
from layoffs_staging2;

select distinct location
from layoffs_staging2;

select distinct country
from layoffs_staging2;

select `date`
from layoffs_staging2;

###3.Removing NULLS/BLANK values
select *
from layoffs_staging2;

select *
from layoffs_staging2
where total_laid_off is null
or total_laid_off ='';

select *
from layoffs_staging2
where percentage_laid_off is null
or percentage_laid_off ='';

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off =''
and percentage_laid_off='';

select *
from layoffs_staging2;
###4.Remove any column or rows

alter table layoffs_staging2
drop column row_num;

select *
from layoffs_staging2;

### 5. Performing some Exploratory Data Analysis(EDA)
select *
from layoffs_staging2
where percentage_laid_off=1
order by funds_raised desc;

select company,sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select min(`date`),max(`date`)
from layoffs_staging2;

select industry,sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

select country,sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

select year(`date`),sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 1 desc;

select stage,sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

select company,sum(percentage_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select substring(`date`,1,7) as MONTH,sum(total_laid_off)
from layoffs_staging2
where substring(`date`,6,2) is not null
group by `month`
order by 1 asc;

with rolling_total as
(
select substring(`date`,1,7) as MONTH,sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,6,2) is not null
group by `month`
order by 1 asc
)
select `month`,total_off,sum(total_off) over(order by `month`)
from rolling_total;

select company,year(`date`),sum(total_laid_off)
from layoffs_staging2
group by company,year(`date`)
order by 3 desc;

with company_year (company,years,total_laid_off) as
(
select company,year(`date`),sum(total_laid_off)
from layoffs_staging2
group by company,year(`date`)
),company_year_rank as 
(select *,dense_rank() over(partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null
)
select *
from company_year_rank
where ranking<=5;