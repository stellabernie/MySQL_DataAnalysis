-- SQL Project - Data Cleaning

-- Data used can be found here:  https://www.kaggle.com/datasets/swaptr/layoffs-2022



SELECT * 
FROM world_layoffs_v1.layoffs;

#CREATING A STAGING TABLE TO WORK WITH

-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens ie. we make a mistake etc
-- Create the new table
CREATE TABLE world_layoffs_v1.layoffs_staging 
LIKE world_layoffs_v1.layoffs;
#insert data into the new table
INSERT layoffs_staging 
SELECT * FROM world_layoffs_v1.layoffs;


#DATA CLEANING STEPS
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. look at null and blank values and see when to populate and when not to 
-- 4. remove any columns and rows that are not necessary - few ways ( A bit more straightforward when not using SQL Workbench)



--  1. ######################################Remove Duplicates#################################################### 

# First let's run the table with its data to find all column names
SELECT *
FROM world_layoffs_v1.layoffs_staging
;


#Then lets run a script to check duplicates
-- What comes as an output are our real duplicates, it's important to include *all columns as part of the criteria for which you check duplicates 
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs_v1.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

#To make sure our script is correct, we are checking 'Yahoo' to confirm its a duplicate. And it is confirmed there are 2 entries for Yahoo
SELECT *
FROM world_layoffs_v1.layoffs_staging
WHERE company LIKE 'Yahoo';


#now you may want to write it like this, if you work in  Microsoft SQL server & PostgreSQL using CTE in one statement. In SQL Workbench, you can't use the below, look at 'Option 2' next section after the syntax below :
#Option 1
WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs_v1.layoffs_staging
) duplicates
WHERE 
	row_num > 1
)
DELETE
FROM DELETE_CTE
;


WITH DELETE_CTE AS (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, 
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM world_layoffs_v1.layoffs_staging
)
DELETE FROM world_layoffs_v1.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
	FROM DELETE_CTE
) AND row_num > 1;

#Option 2
-- A solution for SQL Workbench is to create another table with an extra row we can use as an identifier and deleting all the rows in that table where the identifier shows a value >1.  Then delete the column we used as an identifier. 

ALTER TABLE world_layoffs_v1.layoffs_staging ADD row_num INT;

#check to see if the row was added
SELECT *
FROM world_layoffs_v1.layoffs_staging
;

#create a new table with headings and data types. To do this, navigate to the table on the RHS, right click, 'Send to SQL editor', 'CREATE Statement' & follow the prompts.
CREATE TABLE `world_layoffs_v1`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
`row_num` INT
);

#checking to see if it was created
SELECT *
FROM world_layoffs_v1.layoffs_staging2
;

#verifying the structure of the table, to see what the data types are
DESCRIBE world_layoffs_v1.layoffs_staging2;

#I make a mental note that I have to deal with the data type for date, probably change it to the DATE format but clean up the nulls
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `total_laid_off` VARCHAR(255);

ALTER TABLE layoffs_staging2
MODIFY COLUMN `funds_raised_millions` VARCHAR(255);

#I encountered a problem running the insert statement below. So now I am trouble shooting with the below code.
#ensuring data integrity: making sure columns that are not supposed to have NULL values don't contain them.( I received a 1366 Error for the 'percentage_laid_off' column when wanting to run the INSERT statement) Insteading replacing them with default values.
UPDATE world_layoffs_v1.layoffs_staging
SET percentage_laid_off = 0
WHERE percentage_laid_off IS NULL;

#to identify non numeric numbers
SELECT percentage_laid_off
FROM world_layoffs_v1.layoffs_staging
WHERE NOT percentage_laid_off REGEXP '^[0-9]+(\.[0-9]+)?$';

INSERT INTO `world_layoffs_v1`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs_v1.layoffs_staging;
        
        
-- now that we have this we can delete rows were row_num is greater than 1 or greater and equal to 2

SELECT*
FROM world_layoffs_v1.layoffs_staging2;

DELETE FROM world_layoffs_v1.layoffs_staging2
WHERE row_num >= 2;


-- 2. #########################################Standardize the Data################################################# 

#inspect all rows to identify abnormalities
SELECT * 
FROM world_layoffs_v1.layoffs_staging2;

-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
SELECT DISTINCT industry
FROM world_layoffs_v1.layoffs_staging2
ORDER BY industry;

SELECT *
FROM world_layoffs_v1.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- let's take a look at these
SELECT *
FROM world_layoffs_v1.layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM world_layoffs_v1.layoffs_staging2
WHERE company LIKE 'airbnb%';

-- it looks like airbnb is a travel, but this one just isn't populated. Which may be the same for others.
-- What we can do is, write a query that states that if there is another row with the same company name, it will update it to the non-null industry values
-- This makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE world_layoffs_v1.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- now if we check, we will see that those are all null

SELECT *
FROM world_layoffs_v1.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- now we need to populate those nulls if possible

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM world_layoffs_v1.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- ---------------------------------------------------

-- I also noticed the Crypto title has multiple different variations. We need to standardize that
SELECT DISTINCT industry
FROM world_layoffs_v1.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- now that's taken care of, lets just double check all titles are distinct:
SELECT DISTINCT industry
FROM world_layoffs_v1.layoffs_staging2
ORDER BY industry;

#double check
SELECT *
FROM world_layoffs_v1.layoffs_staging2;

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
#we run this query just to confirm what we will be expecting prior to making any updates
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM world_layoffs_v1.layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- now if we run this again it is fixed
SELECT DISTINCT country
FROM world_layoffs_v1.layoffs_staging2
ORDER BY country;


-- Ideally, we want to  fix the date columns, it's format and type, using the below syntax:
			SELECT *
			FROM world_layoffs_v1.layoffs_staging2;

			-- we can use str to date to update this field
			UPDATE layoffs_staging2
			SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');


			SHOW COLUMNS FROM layoffs_staging2 WHERE Field = 'date';


			-- now we can convert the data type properly
			ALTER TABLE layoffs_staging2
			MODIFY COLUMN `date` DATE;


-- But for this data set, it did not work. Thus I converted the column type to VARCHAR instead of text. I was not able to convert the date format becasue some NULL values are present
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` VARCHAR(10);

-- Then I went with plan B: You can handle the NULL values by using a CASE statement to conditionally apply the STR_TO_DATE() function only to non-NULL values. With the below syntax:
-- Still got an error for updating the date format, a possible reason for this error could be that there are non-NULL values in the date column that don't match the specified format '%m/%d/%Y', causing the STR_TO_DATE() function to fail, thus the added WHERE function
-- In this query, the REGEXP condition filters out rows where the date column does not match the expected format 'MM/DD/YYYY'. The regular expression pattern can be adjusted if your date format differs
UPDATE layoffs_staging2 
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y') 
WHERE `date` IS NOT NULL 
AND `date` REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$';


SELECT *
FROM world_layoffs_v1.layoffs_staging2;



-- 3. ########################################Look at Null Values################################################## We want to populate blank values with data we can infer from the dataset

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal, I prefer to have them null because it makes it easier for calculations during the EDA phase
-- so there isn't anything I want to change with the null values




-- 4. #########################################Remove any needed columns and rows#################################################

SELECT *
FROM world_layoffs_v1.layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM world_layoffs_v1.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM world_layoffs_v1.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs_v1.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM world_layoffs_v1.layoffs_staging2;