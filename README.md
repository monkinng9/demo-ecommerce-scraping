# Programming and Data Orchestration Exam (3 Days)

**Instructions:**

1.  You are required to write code in any programming language of your choice to perform web scraping on at least 2 e-commerce websites.
2.  The objective is to crawl product data from these websites and compare prices for 100 products or more.
3.  Utilize any data orchestration platform such as Dagster, Airflow, or others to manage and schedule your data processing workflow.
4.  The output should be in the form of an Excel spreadsheet (xlsx).
5.  Automate the process to upload the output to Google Drive daily.
    a.  Please attach service account credential in git project
    b.  Please shared uploaded folder to apisit.hen@ascendcorp.com
6.  Implement the solution as a Docker container.
7.  Upload your code to github, gitlab or others and attach link in email

---

# Walkthrough
- เราเลือกมา 3 website
  - true-shopping.com
  - shop.line
- Scrap รายชื่อสินค้าจาก Line Shopping มา 200 รายการ
- จากนั้นทำการ Extract keyword จากชื่อสินค้า
- ร้านค้า
  - Vistra 55
  - BLACKMORES 36 
  - Dettol 29
  - Swisse 21
  - eucerin

https://www.watsons.co.th/en/search?text=Dettol