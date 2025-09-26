-- ============================================================
-- Exploratory Data Analysis on Layoffs Dataset
-- 1. Basic Overview
-- 2. High-Level Aggregation
-- 3. Time-Based Analysis
-- 4. Company-Level Analysis
-- 5. Industry-Level Analysis
-- 6. Country-Level Analysis
-- 7. Stage Analysis
-- 8. Volatility Analysis
-- ============================================================



-- ============================================================
-- 1. Basic Overview
-- ============================================================

-- View the full dataset
SELECT *
FROM layoffs_staging2;

-- Maximum number of employees laid off and maximum layoff percentage
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;



-- ============================================================
-- 2. High-Level Aggregation
-- ============================================================

-- First and last layoff dates in dataset
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- Companies w/ 100% layoffs, sorted by total laid off
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- Companies w/ 100% layoffs, sorted by total funds raised
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Total layoffs by company
SELECT company, SUM(total_laid_off) AS sum_total_laid_off
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Total layoffs by industry
SELECT industry, SUM(total_laid_off) AS sum_total_laid_off
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- Total layoffs by country
SELECT country, SUM(total_laid_off) AS sum_total_laid_off
FROM layoffs_staging2
GROUP BY country
ORDER BY 1 DESC;

-- Total layoffs by stage
SELECT stage, SUM(total_laid_off) AS sum_total_laid_off
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;



-- ============================================================
-- 3. Time-Based Analysis
-- ============================================================

-- Total layoffs by individual dates
SELECT `date`, SUM(total_laid_off) AS sum_total_laid_off
FROM layoffs_staging2
GROUP BY `date`
ORDER BY 1 DESC; 
-- Not reliable because it is each individual date reported (i.e., too much messy data), but nonetheless still good to look at

-- Total layoffs by year
SELECT YEAR(`date`), SUM(total_laid_off) AS sum_total_laid_off
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;
-- Opposite problem of indivdual dates

-- Total layoffs aggregated by year and month
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS sum_total_laid_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

-- CTE, rolling total of layoffs by year and month
WITH Rolling_Total AS
(
	SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS total_off
	FROM layoffs_staging2
	WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
	GROUP BY `MONTH`
	ORDER BY 1 ASC
)
SELECT `MONTH`, total_off,
SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;



-- ============================================================
-- 4. Company-Level Analysis
-- ============================================================

-- Re-checking total layoffs by company
SELECT company, SUM(total_laid_off) AS sum_total_laid_off
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Total layoffs by company and year
SELECT company, YEAR(`date`) AS years, SUM(total_laid_off) AS sum_total_laid_off
FROM layoffs_staging2
GROUP BY company, years
ORDER BY 3 DESC;

-- CTE, top 5 companies per year ranked by total layoffs
WITH Company_Year (company, years, total_laid_off) AS
(
	SELECT company, YEAR(`date`), SUM(total_laid_off)
	FROM layoffs_staging2
	GROUP BY company, YEAR(`date`)
),
Company_Year_Rank AS
(
	SELECT *,
	DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
	FROM Company_Year
	WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;

-- CTE & Join, top 5 companies per year ranked by total layoffs (Horizontal View)
WITH Company_Year (company, years, total_laid_off) AS
(
	SELECT company, YEAR(`date`) AS years, SUM(total_laid_off) AS total_laid_off
	FROM layoffs_staging2
	GROUP BY company, years
),
Company_Year_Rank AS
(
	SELECT *,
	DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
	FROM Company_Year
	WHERE years IN (2020, 2021, 2022, 2023)
),
Top5 AS
(
	SELECT *
	FROM Company_Year_Rank
	WHERE Ranking <= 5
)
SELECT
	t2020.Ranking AS Ranking,
	t2020.company AS "2020 Company",
	t2020.total_laid_off AS "2020 Laid Off",
	t2021.company AS "2021 Company",
	t2021.total_laid_off AS "2021 Laid Off",
	t2022.company AS "2022 Company",
	t2022.total_laid_off AS "2022 Laid Off",
	t2023.company AS "2023 Company",
	t2023.total_laid_off AS "2023 Laid Off"
FROM Top5 t2020
LEFT JOIN Top5 t2021
    ON t2020.Ranking = t2021.Ranking
    AND t2021.years = 2021
LEFT JOIN Top5 t2022
    ON t2020.Ranking = t2022.Ranking
    AND t2022.years = 2022
LEFT JOIN Top5 t2023
    ON t2020.Ranking = t2023.Ranking
    AND t2023.years = 2023
WHERE t2020.years = 2020
ORDER BY t2020.Ranking;



-- ============================================================
-- 5. Industry-Level Analysis
-- ============================================================

-- Total layoffs by industry and year
SELECT industry, YEAR(`date`) AS years, SUM(total_laid_off) AS sum_total_laid_off
FROM layoffs_staging2
GROUP BY industry, years
ORDER BY 3 DESC;

-- CTE, top 5 industries per year ranked by total layoffs
WITH Industry_Year (industry, years, total_laid_off) AS
(
	SELECT industry, YEAR(`date`), SUM(total_laid_off)
	FROM layoffs_staging2
	GROUP BY industry, YEAR(`date`)
),
Industry_Year_Rank AS
(
	SELECT *,
	DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
	FROM Industry_Year
	WHERE years IS NOT NULL
)
SELECT *
FROM Industry_Year_Rank
WHERE Ranking <= 5;

-- CTE & Join, top 5 industries per year ranked by total layoffs (Horizontal View)
WITH Industry_Year (industry, years, total_laid_off) AS
(
	SELECT industry, YEAR(`date`) AS years, SUM(total_laid_off) AS total_laid_off
	FROM layoffs_staging2
	GROUP BY industry, years
),
Industry_Year_Rank AS
(
	SELECT *,
	DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
	FROM Industry_Year
	WHERE years IN (2020, 2021, 2022, 2023)
),
Top5 AS
(
	SELECT *
	FROM Industry_Year_Rank
	WHERE Ranking <= 5
)
SELECT
	t2020.Ranking AS Ranking,
	t2020.industry AS "2020 Industry",
	t2020.total_laid_off AS "2020 Laid Off",
	t2021.industry AS "2021 Industry",
	t2021.total_laid_off AS "2021 Laid Off",
	t2022.industry AS "2022 Industry",
	t2022.total_laid_off AS "2022 Laid Off",
	t2023.industry AS "2023 Industry",
	t2023.total_laid_off AS "2023 Laid Off"
FROM Top5 t2020
LEFT JOIN Top5 t2021
    ON t2020.Ranking = t2021.Ranking
    AND t2021.years = 2021
LEFT JOIN Top5 t2022
    ON t2020.Ranking = t2022.Ranking
    AND t2022.years = 2022
LEFT JOIN Top5 t2023
    ON t2020.Ranking = t2023.Ranking
    AND t2023.years = 2023
WHERE t2020.years = 2020
ORDER BY t2020.Ranking;



-- ============================================================
-- 6. Country-Level Analysis
-- ============================================================

-- Total layoffs by country and year
SELECT country, YEAR(`date`) AS years, SUM(total_laid_off) AS sum_total_laid_off
FROM layoffs_staging2
GROUP BY country, years
ORDER BY 3 DESC;

-- CTE, top 5 countries per year ranked by total layoffs
WITH Country_Year (country, years, total_laid_off) AS
(
	SELECT country, YEAR(`date`), SUM(total_laid_off)
	FROM layoffs_staging2
	GROUP BY country, YEAR(`date`)
),
Country_Year_Rank AS
(
	SELECT *,
	DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
	FROM Country_Year
	WHERE years IS NOT NULL
)
SELECT *
FROM Country_Year_Rank
WHERE Ranking <= 5;

-- CTE & Join, top 5 countries per year ranked by total layoffs (Horizontal View)
WITH Country_Year AS
(
    SELECT country, YEAR(`date`) AS years, SUM(total_laid_off) AS total_laid_off
    FROM layoffs_staging2
    GROUP BY country, YEAR(`date`)
),
Country_Year_Rank AS
(
    SELECT *,
	DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
    FROM Country_Year
    WHERE years IN (2020, 2021, 2022, 2023)
),
Top5 AS
(
    SELECT *
    FROM Country_Year_Rank
    WHERE Ranking <= 5
)
SELECT
	t2020.Ranking AS Ranking,
	t2020.country AS "2020 Country",
	t2020.total_laid_off AS "2020 Laid Off",
	t2021.country AS "2021 Country",
	t2021.total_laid_off AS "2021 Laid Off",
	t2022.country AS "2022 Country",
	t2022.total_laid_off AS "2022 Laid Off",
	t2023.country AS "2023 Country",
	t2023.total_laid_off AS "2023 Laid Off"
FROM Top5 t2020
LEFT JOIN Top5 t2021
    ON t2020.Ranking = t2021.Ranking
    AND t2021.years = 2021
LEFT JOIN Top5 t2022
    ON t2020.Ranking = t2022.Ranking
    AND t2022.years = 2022
LEFT JOIN Top5 t2023
    ON t2020.Ranking = t2023.Ranking 
	AND t2023.years = 2023
WHERE t2020.years = 2020
ORDER BY t2020.Ranking;



-- ============================================================
-- 7. Stage Analysis
-- ============================================================

-- Total layoffs aggregated by stage and year
SELECT stage,
	YEAR(`date`) AS years,
    SUM(total_laid_off) AS sum_total_laid_off
FROM layoffs_staging2
WHERE stage IS NOT NULL
	AND YEAR(`date`) IS NOT NULL
GROUP BY stage, YEAR(`date`)
ORDER BY YEAR(`date`), stage;

-- Ranking stages by total layoffs each year
WITH Stage_Year (stage, years, total_laid_off) AS
(
	SELECT stage,
		YEAR(`date`),
		SUM(total_laid_off)
	FROM layoffs_staging2
	WHERE stage IS NOT NULL
		AND YEAR(`date`) IS NOT NULL
	GROUP BY stage, YEAR(`date`)
	ORDER BY YEAR(`date`), stage
),
Stage_Year_Rank AS
(
	SELECT *,
	DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
	FROM Stage_Year
	WHERE years IS NOT NULL
)
SELECT *
FROM Stage_Year_Rank;



-- ============================================================
-- 8. Volatility Analysis
-- ============================================================

-- Industry Volatility
SELECT industry,
	FORMAT(STDDEV(total_laid_off), 2) AS Volatility
FROM layoffs_staging2
WHERE industry IS NOT NULL
GROUP BY industry
ORDER BY STDDEV(total_laid_off) DESC;