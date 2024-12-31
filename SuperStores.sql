### link-https://www.kaggle.com/datasets/ishanshrivastava28/superstore-sales
###1.Cleaning the data by removing duplicate rows
select *
from superstore;

create table superstore2
like superstore;

insert into superstore2
select *
from superstore;

select *
from superstore2;

select *,
row_number() over(
partition by `row id`,`order id`,`order date`,`ship date`,`customer id`,`customer name`,segment,country,city,`postal code`,region,`product id`,category,`product name`,sales,quantity,discount,profit) as row_num
from superstore2;

with duplicate_cte as
(
select *,
row_number() over(
partition by `row id`,`order id`,`order date`,`ship date`,`customer id`,`customer name`,segment,country,city,`postal code`,region,`product id`,category,`product name`,sales,quantity,discount,profit) as row_num
from superstore2
)
select *
from duplicate_cte
where row_num>1;

###Note:That there's NO duplicate in any of the rows
###2.Standardizing the data
select *
from superstore2;

select distinct(`row id`)
from superstore2;

select distinct(`order id`)
from superstore2;

select distinct(`order date`)
from superstore2;

select distinct(`ship date`)
from superstore2;

select distinct(`customer id`)
from superstore2;

select distinct(`customer name`)
from superstore2;

select distinct(segment)
from superstore2;

select distinct(country)
from superstore2;

select distinct(city)
from superstore2;

select distinct(`postal code`)
from superstore2;

select distinct(region)
from superstore2;

select distinct(`product id`)
from superstore2;

select distinct(category)
from superstore2;

select distinct(`product name`)
from superstore2;

select distinct(sales)
from superstore2;

select distinct(quantity)
from superstore2;

select distinct(discount)
from superstore2;

select distinct(profit)
from superstore2;

###Note:From visual inspection all the data in the table came Standardized that 
### is to say that all the data were in the SAME FORMAT & no spelling error

###3.Removing Null/Blank values

select *
from superstore2;

select *
from superstore2
where Sales is null;

select *
from superstore2
where Quantity is null;

select *
from superstore2
where Discount is null;

select *
from superstore2
where profit is null;

###Note:That there is NO NULL or BLANK values in those columns

###5.Removing rows and columns that are duplicate
### Note:From my analysis the data was cleaned before uploaded to the website
### so they no rows/columns to delete or remove

###Phase 2
### Performong Exploratory Data Analysis
select *
from superstore2;

select max(sales),min(sales),max(Quantity),min(Quantity),max(Discount),min(Discount),max(Profit),min(profit)
from superstore2;

select sales,`Customer Name`
from superstore2
where Sales=22638.48;

select Quantity,`customer name`
from superstore2
where Quantity=14;

select max(`order date`),min(`order date`),max(`ship date`),min(`ship date`)
from superstore2;

select max(`order date`),min(`order date`),max(`ship date`),min(`ship date`)
from superstore2
group by `Order Date`;


select *
from superstore2;

alter table superstore2
add Total_sales int after Quantity;

update superstore2 
set Total_sales = Sales * Quantity;

