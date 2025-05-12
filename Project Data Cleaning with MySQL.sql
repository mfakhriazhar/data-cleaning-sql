-- Data Cleaning Project with MySQL --

SELECT *
FROM layoffs_data;

-- GOALS --
-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Handle Null Values or blank values
-- 4. Remove Any Columns

-- First, I create a table first so that the raw table is not disturbed and not changed.
CREATE TABLE layoffs_staging
LIKE layoffs_data;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
Select *
FROM layoffs_data;

-- 1. Remove Duplicate Data
-- There are several ways to see duplicate data but I think using ROW_NUMBER is one of the easiest.
Select *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`)
AS row_num
FROM layoffs_staging;

-- I make cte to handle duplicate data to make it more secure.
WITH duplicate_cte AS
(Select *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions)
AS row_num
FROM layoffs_staging)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- From here we know that data with ROW_NUM above 1 is duplicate data, because ROW_NUM = 1 is only a unique value.
-- we take a sample from Cazoo, then we can see that there are indeed duplicate data
SELECT *
FROM layoffs_staging
WHERE company = 'Cazoo';

-- I created another table that we match the query in the cte by adding the row_num column in it.
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
Select *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions)
AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- After checking and rechecking and we are sure that the value is indeed duplicate data, 
-- we execute it by deleting it from the table.
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- voila there is no duplicate data anymore
SELECT *
FROM layoffs_staging2;

-- 2. Standardizing Data
-- Here we will clean up the data formatting in the table like removing white space, then fix the values that have issues, etc.
SELECT DISTINCT(company)
FROM layoffs_staging2;

-- in this company there are some values that contain white space so let's execute it using TRIM.
SELECT company, TRIM(company)
FROM layoffs_staging2;

-- If it's confirmed to be neat, we just need to update it.
UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT *
FROM layoffs_staging2;

-- we check in the industry
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- it turns out that there are several values that should be the same but different so there is crypto, crypto currency etc. 
-- so we just execute it immediately.
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- let's just update everything to Crypto only
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Let's check again
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- yep that's done, let's check the other columns

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;
-- well we found out that there are United States and United States. let's execute it again

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%';

-- we remove the '.' in the value with trailing
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

-- It's working, let's update the table again
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT *
FROM layoffs_staging2;

-- next we change the data type of the date column from text to date time to make it easier for time series analysis.
SELECT `date`
FROM layoffs_staging2;

-- we change the format to the standard date format in MySQL
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- let's update the table again
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

-- After that, we change the data type from text to date using alter table so that it doesn't change the real raw data/table.
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

DESCRIBE layoffs_staging2;

SELECT *
FROM layoffs_staging2;

-- 3. Handle Null and blank values
-- check for NULL values in the total_laid_off and percentage_laid_off variables
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- it turns out that there are a lot of NULLs and later we will check again and make sure whether we will delete or populate the data.
-- we check in the industry first, because earlier it seemed like there were blank values, let's check and execute it.
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- let's check one of the companies
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- It turns out that Airbnb is a company engaged in travel, so let's populate the blank values with the same values as the company.
-- The solution is to join the table itself by updating the blank values with non-blank values.

SELECT t1.industry, t2.industry
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- from here we can know that Airbnb is travel, Carvana is transportation and Juul is Consumer
-- Now it's time to update the blank values in t1 with non-blank values in t2.

UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- done! now we check again
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb'
OR company = 'Carvana'
OR company = 'Juul';

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';
-- and if we check it looks like Bally's was the only one without a populated row to populate this null values

-- next kita handle null value tadi, karena sepertinya kita tidak bisa ngepopulate datanya karena kita gapunya kolom yang 
-- can be a reference column for us to populate the data like company total or original table before laid off so we can calculate the data.
-- so I think we can just delete it

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- 4. Remove any columns
SELECT *
FROM layoffs_staging2;

-- because we don't need the raw_num column anymore, we can just drop the column using the alter table.
-- we can delete some columns according to our needs too

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
