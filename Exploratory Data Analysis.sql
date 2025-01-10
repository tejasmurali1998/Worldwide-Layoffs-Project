select * from layoffs_staging_2;

select max(total_laid_off) , max(percentage_laid_off) from layoffs_staging_2;

select * from layoffs_staging_2 where percentage_laid_off = 1
order by total_laid_off desc; 

select * from layoffs_staging_2 where percentage_laid_off = 1
order by funds_raised_millions desc;

select company, sum(total_laid_off) as total_layoffs_per_company 
from layoffs_staging_2
group by company 
order by 2 desc;

select min(date) as earliest_date, max(date) as latest_date from layoffs_staging_2;

select industry, sum(total_laid_off) as total_layoffs_per_industry 
from layoffs_staging_2
group by industry 
order by 2 desc;

select country, sum(total_laid_off) as total_layoffs_per_country 
from layoffs_staging_2
group by country
order by 2 desc;

select * from layoffs_staging_2;

select Year(date), sum(total_laid_off) as total_layoffs_per_country 
from layoffs_staging_2
group by Year(date)
order by 1 desc;

select stage, sum(total_laid_off) as total_layoffs_per_country 
from layoffs_staging_2
group by stage
order by 2 desc;

select substring(date, 1, 7) as month, sum(total_laid_off) from layoffs_staging_2 
where substring(date, 1, 7) is not null
group by 1 order by 1 asc;

select * from layoffs_staging_2 order by total_laid_off desc;

With Rolling_Total as (select substring(date, 1, 7) as month, 
sum(total_laid_off) as total_off from layoffs_staging_2 
where substring(date, 1, 7) is not null
group by 1 order by 1 asc)
select month, total_off, sum(total_off) over(order by month) as rolling_total from Rolling_Total;

select company, year(date) as year, sum(total_laid_off) as total_off from layoffs_staging_2
group by company, year(date) order by 3 desc;

with company_year (company, years, total_laid_off) as (select company, year(date), sum(total_laid_off) from layoffs_staging_2
group by company, year(date)), company_year_rank as 
(select *, 
dense_rank() over(partition by years order by total_laid_off desc) as Ranking from company_year 
where years is not NULL)
select * from company_year_rank where Ranking <= 5;



































