### What is this about

This is a basic web scraper that goes to https://en.wikipedia.org/wiki/List_of_largest_Polish_companies and pulls out useful information like:

- Table with column names and rows
- Shows Top Polish Companies by their revenue
- Cleanse the data

It saves everything neatly into a .xlsx file so you can open it in Excel, Google Sheets, or use it for further analysis.

I made it for learning purposes — based on Alex The Analyst courses — nothing too complicated, just clean, working code that gets real data from the web page.
![Screenshot](https://github.com/pavddOpe/pav-data-analytics-portfolio/blob/main/06_Web_Scraping_w_PY/pics/PY_table.jpg)

### Key Things I Learned

- How to inspect a webpage (right-click → Inspect) to find the right CSS selectors / tags
- Using `requests` to download pages and `BeautifulSoup` to make them searchable
- Writing helper functions so the code stays readable and reusable
- Basic error handling (what if the site is down? what if a field is missing?)
- Exporting structured data with `pandas` or plain `xlsx` module
- Why websites sometimes block scrapers (User-Agent, headers) and simple ways to look more like a browser

Most importantly: web scraping teaches patience — sites change, selectors break, but debugging that is excellent practice.
