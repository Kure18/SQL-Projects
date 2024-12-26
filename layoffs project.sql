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
