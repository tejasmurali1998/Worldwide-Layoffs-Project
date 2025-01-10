-- Data Cleaning 

select * from layoffs;

-- Remove Duplicates
-- Standardize the Data
-- Deal with NULL or Blank Values
-- Remove Unecessary Columns or Rows 

create table layoffs_staging like layoffs; -- creates identical table structure as original

select * from layoffs_staging; 

insert into layoffs_staging select * from layoffs; -- inserts identical values as original

with duplicate_cte as  
( -- creating CTE with column row_num that shows frequency of records occuring
select *, 
row_number() over(
partition by company, location, 
industry, total_laid_off, percentage_laid_off, 'date', stage, country,
funds_raised_millions) as row_num 
from layoffs_staging
)
select * from duplicate_cte where row_num > 1;

select * from layoffs_staging where company = 'Casper';

with duplicate_cte as  
( -- creating CTE with column row_num that shows frequency of records occuring
select *, 
row_number() over(
partition by company, location, 
industry, total_laid_off, percentage_laid_off, 'date', stage, country,
funds_raised_millions) as row_num 
from layoffs_staging
)
delete from duplicate_cte where row_num > 1;

CREATE TABLE `layoffs_staging_2` (-- turning the CTE output into an actual table
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int(11) DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

ALTER TABLE layoffs_staging_2
ADD COLUMN row_num INT;

select * from layoffs_staging_2;

insert into layoffs_staging_2
select *, 
row_number() over(
partition by company, location, 
industry, total_laid_off, percentage_laid_off, 'date', stage, country,
funds_raised_millions) as row_num 
from layoffs_staging;

select * from layoffs_staging_2 where row_num > 1;
SET SQL_SAFE_UPDATES = 0;
delete from layoffs_staging_2 where company in ('Casper', 'Cazoo', 'Hibob', 'Wildlife Studios', 'Yahoo') 
and row_num = 2; -- these are duplicate rows
SET SQL_SAFE_UPDATES = 1;

select * from layoffs_staging_2;

-- Standardizing Data

select company, trim(company) from layoffs_staging_2;

UPDATE layoffs_staging_2
set company = TRIM(company); -- trimming whitespace

select distinct industry from layoffs_staging_2
order by 1;

select * from layoffs_staging_2 where industry like "%Crypto%";

UPDATE layoffs_staging_2 
set industry = 'Crypto'
where industry like '%Crypto%';

select distinct location from layoffs_staging_2 order by 1;

select distinct country, trim(trailing '.' from country) 
from layoffs_staging_2 -- gets rid of '.' after country name
order by 1;

update layoffs_staging_2 set country = trim(trailing '.' from country)
where country like '%United States%'; -- only the US row needs to be changed

select date, str_to_date(date, '%m/%d/%Y') from layoffs_staging_2; 
-- arguments for str_to_date() are string, format it's in

update layoffs_staging_2 set date = str_to_date(date, '%m/%d/%Y');

select date from layoffs_staging_2;

alter table layoffs_staging_2 modify column date DATE; -- text to date type

select * from layoffs_staging_2;

-- NULL/missing values and blank values

select * from layoffs_staging_2 where total_laid_off is NULL
and percentage_laid_off is NULL; 

select * from layoffs_staging_2 where industry is NULL
or industry = '';

select * from layoffs_staging_2 where company = 'Airbnb';

select t1.industry, t2.industry from layoffs_staging_2 t1 -- see where there is NULL or blank values
join layoffs_staging_2 t2 on t1.company = t2.company 
where (t1.industry is NULL or t1.industry = '')
and t2.industry is not NULL;

update layoffs_staging_2 t1 
join layoffs_staging_2 t2 on t1.company = t2.company -- self join for imputation with known values 
set t1.industry = t2.industry where t1.industry is NULL 
and t2.industry is not NULL;

update layoffs_staging_2 set industry = NULL where industry = '';

select * from layoffs_staging_2 where company like '%Bally%';

select * from layoffs_staging_2; 

select * from layoffs_staging_2 where total_laid_off is NULL
and percentage_laid_off is NULL; 

delete from layoffs_staging_2 where total_laid_off is NULL
and percentage_laid_off is NULL; 

select * from layoffs_staging_2;

alter table layoffs_staging_2 drop column row_num;














