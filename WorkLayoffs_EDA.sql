-- EDA: We cleaned the data and now we can explore the data

-- We will explore the data and find trends or patterns or anything interesting like outliers

-- normally when you start the EDA process you have some idea of what you're looking for. Sometimes you find issues that you need to clean

-- with this data, I am not looking at anything in specific, I am purely aiming to explore it..  look around and see what I find!

SELECT * 
FROM layoffs_staging2;



-- EASY QUERIES
SELECT MAX(total_laid_off)
FROM layoffs_staging2; 

-- If i want to quickly see the data type of a column I can run this
SHOW COLUMNS 
FROM layoffs_staging2 
WHERE Field = 'total_laid_off';

-- I am unable to run the query above because the column's data type if VARCHAR
-- I can't convert the column data type until I sort out the NULL values
-- So I want to convert all NULL's to numeric values then change the column type

-- Step one, This worked
UPDATE layoffs_staging2 
SET total_laid_off = 0 
WHERE total_laid_off IS NULL;

-- Step 2: This does not work, still complaining about NULL values
ALTER TABLE layoffs_staging2 
MODIFY COLUMN total_laid_off INT;


-- so I want to identify the non-numeric values then handle them and change the column data type if needed
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NOT NULL
AND total_laid_off REGEXP '[^0-9]';

-- handle them, in this case I am changing them to 0 
UPDATE layoffs_staging2 
SET total_laid_off = 0 
WHERE total_laid_off REGEXP '[^0-9]';


-- try converting the column to an INT data type again
ALTER TABLE layoffs_staging2 
MODIFY COLUMN total_laid_off INT;

-- now lets do the same for percentage_laid_off !As I can see, the bottom syntax does not execute as expected
ALTER TABLE layoffs_staging2 
MODIFY COLUMN percentage_laid_off DECIMAL(10, 2);

-- again, identify non-numeric values
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off IS NOT NULL
AND percentage_laid_off REGEXP '[^0-9.]';


-- handle the rows in the output
UPDATE layoffs_staging2 
SET percentage_laid_off = NULL 
WHERE percentage_laid_off REGEXP '[^0-9.]';


-- retry altering the column type. Remember in the data cleaning process, I was unable to delete these rows, most likely related to the data type
ALTER TABLE layoffs_staging2 
MODIFY COLUMN percentage_laid_off DECIMAL(10, 2);

-- now lets see if we can run the below
SELECT MAX(total_laid_off)
FROM layoffs_staging2; 
-- perfect, it worked. 

-- let's see if this works. Trying it again from our failed attempt in the data cleaning.
DELETE FROM layoffs_staging2
WHERE total_laid_off = 0 
AND percentage_laid_off IS NULL;
-- fantastic, that worked. 

DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2; 


-- Looking at Percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;

-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY total_laid_off DESC;
-- these are mostly startups it looks like who all went out of business during this time

-- if we order by funcs_raised_millions we can see how big some of these companies were
SELECT *
FROM layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;



-- Let's explore using GROUP BY--------------------------------------------------------------------------------------------------

-- Companies with the biggest single Layoff

SELECT company, total_laid_off
FROM layoffs_staging
ORDER BY 2 DESC
LIMIT 5;
-- now that's just on a single day

-- Companies with the most Total Layoffs
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;


-- lets look at the date range for this data
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- by location
SELECT location, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- this it total in the past 3 years or in the dataset

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(date), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(date)
ORDER BY 1 ASC;


SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;


SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;



-- TOUGHER QUERIES------------------------------------------------------------------------------------------------------------------------------------

-- Let's look at the progession of layoffs, ie. rolling totals of layoffs
-- Earlier we looked at Companies with the most Layoffs. Now let's look at that per year. It's a little more difficult.

-- Rolling Total of Layoffs Per Month
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC;

-- now use it in a CTE so we can query off of it
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, total_laid_off, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;

-- creating a CTE to rank company layoffs

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

-- checking to see which country most of the layoffs occured
SELECT *
FROM  layoffs_staging2;

SELECT country,industry, SUM(total_laid_off) AS layoffs
FROM  layoffs_staging2
GROUP BY country, industry
HAVING layoffs > 0
ORDER BY layoffs DESC;












































