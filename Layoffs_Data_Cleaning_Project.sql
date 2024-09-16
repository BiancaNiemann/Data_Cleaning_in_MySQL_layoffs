-- Data Cleaning

SELECT *
FROM layoffs;

-- 1. Remove any duplicates
-- 2. Standarize the Data
-- 3. Null Values or blank Values
-- 4. Remove any columns


-- Create a staging table so you have the raw data as a backup, always better not to work on raw data
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

-- 1.Remove the Duplicates
-- Add row number to all rows and if duplicate it will have number 2

SELECT *, 
	ROW_NUMBER() OVER (
		PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    ) AS 'row_num'
FROM layoffs_staging;

WITH duplicate_cte AS (
	SELECT *, 
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
		) AS 'row_num'
	FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Cant just delete the rows above once found, need to create a new table
-- Create empty table (Can right click table on left and copy to clipboard - select all) then add a col called row nums
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

-- Copy the data from first staging table to 2 table
INSERT INTO layoffs_staging2
SELECT *, 
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
		) AS 'row_num'
	FROM layoffs_staging;

-- Delete the duplicate rows, they will have row num 2 or more
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Standardizing Data (Finding and fixing data)

-- Create temp col to see difference
SELECT COMPANY, TRIM(company)
FROM layoffs_staging2;

-- Update the actual column in the table
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Look at industry col
SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;

-- Can see 3 diff types for the crypto, nned them to all be named the same so they group together later
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- Update the name to only be Crypto
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Check only one name now
SELECT DISTINCT(industry)
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- Check location
SELECT DISTINCT(location)
FROM layoffs_staging2
ORDER BY 1;

-- Look at country
SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER BY 1;

-- Found that United states has 2, one with full stop at end
SELECT DISTINCT(country)
FROM layoffs_staging2
WHERE country LIKE 'United States%';

-- Update the country so doesnt include '.'
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Checked it worked
SELECT DISTINCT(country)
FROM layoffs_staging2
WHERE country LIKE 'United States%';

-- Date col is set to text col which is not good, need to adjust to be mm/dd/yyy
SELECT `date`,
	STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- Update the col
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Convert column from text to date (Only ever use Alter on copy table never original)
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Working with null and blank values

-- Found 3 blanks and 1 null in the industry column
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- Look at the above results and see one missing from Airbnb so select that company
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

-- Can now see the missing industry is Travel, need to update row

SELECT *
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Need to update to NULL where blank first
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Update the industry
UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Remove cols and Rows where not needed

-- These rows have both laid off and percentage laid off as null, therefore cannot be used - before deleting anything always make sure you are confident ok to delete
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
	AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
	AND percentage_laid_off IS NULL;
    
-- delete the row_num col we made, dont need anymore

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;






