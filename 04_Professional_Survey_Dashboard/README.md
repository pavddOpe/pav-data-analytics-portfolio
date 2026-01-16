### 5. Professional Survey Analytics Dashboard (Power BI)
It's need to be mentioned that this project was created based on courses provided by ***"Alex The Analyst"*** tutorial.

**Additional key Data Cleaning & Transformation Highlights added during the work on it (Power Query):**
- Cleaned the “Which title best fits your role?” column by removing “Other (Please specify):” prefixes, splitting by delimiters, replacing values, and applying Trim to remove leading spaces. Result: consistent titles like Data Analyst, Data Scientist, Data Engineer, etc.
- Converted salary range text (e.g., “$100k-$120k”) into usable numeric values:
- Duplicated column → Split by digit/non-digit → Cleaned symbols → Converted to numbers → Created Average Salary custom column by averaging lower and upper bounds.
- Consolidated less common responses and grouped ETL-related answers into a unified category, while keeping top languages distinct and labeling others as “Other”.
- Analyzed free-text “Other” responses in the job change motivation question. Used conditional columns with nested IF and OR logic + Text.Contains to reclassify recurring themes (e.g., “better salary”, “learning new skills”) into meaningful categories.
- Removed unnecessary columns and ensured data types were correct for accurate modeling and visualization.
  
![Main Dashboard](https://github.com/user-attachments/assets/caa4a679-cc0a-487a-905d-7925663bf2d8)
[The Power BI file access](https://github.com/pavddOpe/pav-data-analytics-portfolio/blob/main/04_Professional_Survey_Dashboard/Professional%20Survey%20Analysis.pbix)
