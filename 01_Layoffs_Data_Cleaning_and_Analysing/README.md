# Project 1: Data cleaning - Layoffs 2022

## What was done
- Removed duplicates
- Standardized text (ex. TRIM)
- Cleared NULLs & filling up missing spots.
- Converting data types.
- Additionally: summarizing report, data_quality_score, analitycal view.
- Prepared based on "Alex the analyst Tutorial" with my self extensions and changes.

![filtering2](https://github.com/user-attachments/assets/6856c026-dc31-4dbe-a7e5-8d8000ccc894)
![rolling6](https://github.com/user-attachments/assets/6e511ec2-5ce7-4d04-991b-5dc479a73e8f)

## Files
- Layoffs_Cleaning_Project - full code[Full_Code](01_Layoffs_Data_Cleaning_and_Analysing/Layoffs_Cleaning_Project.sql)
- layoffs_original_dataset.csv - raw data[Dataset](01_Layoffs_Data_Cleaning_and_Analysing/layoffs_original_dataset.csv)


# Project 2: Exploratory Data Analysis Project - Layoffs 2022

## What was done
After the cleaning I did deeper analyze of the data:
- Max layoffs (full and percentage).
- Companies with 100% layoffs.
- Top companies, industries and stages by the calculation of layoffs.
- Timetrends - by year and month
- Added Rolling total (cumulated sum of layoffs in time) with WINDOW FUNCTIONS
- RANK of top 5 companies with the biggest layoffs (CTE+DENSE_RANK)

![Editing_the_data_2](https://github.com/user-attachments/assets/8412af64-7f3f-49ed-a2b6-ad4a3da2788a)
![Joining3](https://github.com/user-attachments/assets/518333aa-4d8e-494e-89db-99bd8bcc2f38)
![Final_report6](https://github.com/user-attachments/assets/021c423d-023c-41f3-b799-3c5308a90549)

## Files
= Layoffs_Exploratory_Data_Analysis_Project - full code[Full_Code](01_Layoffs_Data_Cleaning_and_Analysing/Layoffs_Exploratory_Data_Analysis_Project.sql)
- layoffs_original_dataset.csv - raw data[Dataset](01_Layoffs_Data_Cleaning_and_Analysing/layoffs_original_dataset.csv)

New queries will be added in the nearest future :)


