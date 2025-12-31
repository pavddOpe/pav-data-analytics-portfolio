-- DATA CLEANING PROJECT 1

SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null or Blank values
-- 4. Remove Any columns



CREATE TABLE lalayoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;


SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;


WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num >1;

SELECT *
FROM layoffs_staging
WHERE company IN ('Oda','Terminus')
ORDER BY company;


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

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;


-- Standardizing data

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country
FROM layoffs_staginsg2
WHERE country LIKE 'United St%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country = 'United States.';

SELECT `date`
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- NULL and BLANK values

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';


UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT t1.company, t1.location, t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;


-- changing percentages data type

ALTER TABLE layoffs_staging2
MODIFY COLUMN percentage_laid_off FLOAT;

-- change NULLs to 'No data'

SELECT 
	company,location,industry,
	CONCAT(IFNULL(total_laid_off, 'No data')) AS total_laid_off_display,
	CONCAT(IFNULL(percentage_laid_off, 'No data')) AS percentage_laid_off_display, -- as we don't want to change the type of column (from INT to STR)
	`date`,stage,country,funds_raised_millions   
FROM layoffs_staging2;

SELECT GROUP_CONCAT(column_name ORDER BY ordinal_position)  -- how to choose all your column names and create a new view if needed
FROM information_schema.columns
WHERE table_schema = 'world_layoffs'
AND table_name = 'layoffs_staging2';

CREATE VIEW layoffs_staging2_revised AS
SELECT company,location,industry,percentage_laid_off,`date`,stage,country,funds_raised_millions
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2_revised;



-- Project End Report
SELECT 
	company,location,industry,
	CONCAT(IFNULL(total_laid_off, 'No data')) AS total_laid_off_display,
	CONCAT(IFNULL(percentage_laid_off, 'No data')) AS percentage_laid_off_display, -- as we don't want to change the type of column (from INT to STR)
	`date`,stage,country,funds_raised_millions
FROM layoffs_staging2;


-- 1. Summary:
SELECT
    '= DATA CLEANING SUMMARY =' AS company,
    'NULLs replaced with "No data"' AS location,
    '' AS industry,
    CONCAT(
        (SELECT COUNT(*) FROM layoffs WHERE total_laid_off IS NULL),
        ' → 0'
    ) AS total_laid_off_display,
    CONCAT(
        (SELECT COUNT(*) FROM layoffs WHERE percentage_laid_off IS NULL),
        ' → 0'
    ) AS percentage_laid_off_display,
    '' AS `date`,
    CONCAT(
		(SELECT COUNT(*) FROM layoffs WHERE stage IS NULL),
		' → 0'
    ) AS stage,
    '' AS country,
    CONCAT(
		(SELECT COUNT(*) FROM layoffs WHERE funds_raised_millions IS NULL),
        ' → 0'
	) AS funds_raised_millions,
    '' AS data_quality_score

UNION ALL
-- 2. Header row
SELECT
    'Company' AS company,
    'Location' AS location,
    'Industry' AS industry,
    'Total Laid Off' AS total_laid_off_display,
    'Percentage Laid Off' AS percentage_laid_off_display,
    'Date' AS `date`,
    'Stage' AS stage,
    'Country' AS country,
    'Funds Raised' AS funds_raised_millions,
    'Data Quality Score' AS data_quality_score

UNION ALL
-- 3. Cleaned data
SELECT
    company,
    location,
    industry,
    IFNULL(total_laid_off, 'No data') AS total_laid_off_display,
    IFNULL(percentage_laid_off, 'No data') AS percentage_laid_off_display,
    `date`,
    IFNULL(stage, 'Unknown') AS stage,
    country,
    IFNULL(funds_raised_millions, 'No data') AS funds_raised_millions,
    data_quality_score
FROM layoffs_staging2;



-- adding data quality score

ALTER TABLE layoffs_staging2
ADD COLUMN data_quality_score INT DEFAULT 0;

UPDATE layoffs_staging2
SET data_quality_score = 
	(CASE WHEN total_laid_off IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN percentage_laid_off IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN industry IS NOT NULL AND industry != '' THEN 1 ELSE 0 END) +
    (CASE WHEN stage IS NOT NULL AND stage != 'Unknown' THEN 1 ELSE 0 END) +
    (CASE WHEN funds_raised_millions IS NOT NULL THEN 1 ELSE 0 END);
    
SELECT data_quality_score, COUNT(*)
FROM layoffs_staging2
GROUP BY 1
ORDER BY 1 DESC;

SELECT MAX(data_quality_score), MIN(data_quality_score) 
FROM layoffs_staging2;


CREATE VIEW vw_layoffs_clean AS
SELECT 
	company,
	TRIM(company) AS company_clean,
    location,
    industry,
    IFNULL(total_laid_off, 'No data') AS total_laid_off_display,
    IFNULL(percentage_laid_off, 'No data') AS percentage_laid_off_display,
	`date`,
    YEAR(`date`) AS year,
    MONTH(`date`) AS month,
    IFNULL(stage, 'Unknown') AS stage,
    country,
    IFNULL(funds_raised_millions, 'No data') AS funds_raised_millions,
    data_quality_score
FROM layoffs_staging2;

SELECT *
FROM vw_layoffs_clean;

UPDATE layoffs_staging2
SET company = 'Airtime'
WHERE company LIKE 'Airtime%';


-- some calculation of difference removed dups
SELECT
	(SELECT COUNT(*) FROM layoffs) AS initial_data,
    (SELECT COUNT(*) FROM layoffs_staging2) AS after_cleaning_data,
    (SELECT COUNT(*) FROM layoffs) - (SELECT COUNT(*) FROM layoffs_staging2) AS removed_dups;



-- Just to simplify for the future automatizations and optimizations

DELIMITER //
CREATE PROCEDURE CleanLayoffsData()
BEGIN
	DELETE t1 FROM layoffs_staging2 t1
    INNER JOIN layoffs_staging2 t2
    WHERE
		t1.company = t2.company AND
        t1.location = t2.location AND
        t1.`date` = t2.`date` AND
        t1.total_laid_off = t2.total_laid_off;
        
	UPDATE layoffs_staging2
    SET country = 'United States'
    WHERE country LIKE 'United States%';
    
    UPDATE layoffs_staging2 t1
    JOIN layoffs_staging2 t2 ON t1.company = t2.company
    SET t1.industry = t2.industry
    WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;
END //
DELIMITER ;


-- For a Quick visualization
SELECT 
    company,
    SUM(total_laid_off) AS total_laid_off_count,
    country
FROM layoffs_staging2
WHERE total_laid_off IS NOT NULL
GROUP BY company, country
ORDER BY total_laid_off_count DESC
LIMIT 30;

