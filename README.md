# SQL_E-Commerce_Dataset
# I. Introduction
This project contains an eCommerce dataset that I will explore using SQL on Google BigQuery. The dataset is based on the Google Analytics public dataset and contains data from an eCommerce website.
# II. Requirement
    - Google Cloud Platform account
    - Project on Google Cloud Platform
    - Google Bigquery API enabled
    - SQL query editor or IDE
# III. Dataset Access
The eCommerce dataset is available in a public Google BigQuery dataset. To access it, complete the following steps:
   - Sign in to your Google Cloud Platform account and create a new project.
   - Open the BigQuery console and choose the project you just created.
   - From the navigation menu, click Add Data, then select Search a project.
   - Enter the project ID bigquery-public-data.google_analytics_sample.ga_sessions and press Enter.
   - Click on the ga_sessions_* table to view the data.

https://support.google.com/analytics/answer/3437719?hl=en
<img width="924" height="712" alt="Image" src="https://github.com/user-attachments/assets/e962f770-00d9-4764-b1d0-55a2832677dd" />
# IV. Explore the Dataset
In this project, I will write 08 query in Bigquery base on Google Analytics dataset
# Query 01: Calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)
- SQL code
<img width="770" height="181" alt="Image" src="https://github.com/user-attachments/assets/73020b99-d796-4e93-9c4e-09cf4751c8bc" />

- Query results
<img width="792" height="135" alt="Image" src="https://github.com/user-attachments/assets/597d7389-d8a2-4372-be8c-8c3507f27a74" />

# Query 02: Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC)
- SQL code
<img width="786" height="162" alt="Image" src="https://github.com/user-attachments/assets/75920684-bc5e-4260-af5f-cd217e0ec931" />

- Query results
<img width="788" height="536" alt="Image" src="https://github.com/user-attachments/assets/1ac4919d-ea51-44f8-a998-0690dac8be25" />

# Query 3: Revenue by traffic source by week, by month in June 2017
- SQL code
<img width="772" height="637" alt="Image" src="https://github.com/user-attachments/assets/984fc33a-7c3e-4142-8192-9d5dfa7a17d5" />

- Query results
<img width="983" height="707" alt="Image" src="https://github.com/user-attachments/assets/fadc1699-ff44-4390-bbf0-6ec48e9dc778" />
# Query 04: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017
- SQL code
<img width="995" height="657" alt="image" src="https://github.com/user-attachments/assets/55345e6e-c0fe-450f-82b6-34b583e5d77e" />

- Query results
<img width="778" height="102" alt="image" src="https://github.com/user-attachments/assets/e5189696-3632-4b3c-9158-50a2fcccfcfc" />
# Query 05: Average number of transactions per user that made a purchase in July 2017
- SQL code
<img width="947" height="183" alt="image" src="https://github.com/user-attachments/assets/11fe034f-bfde-47a3-a565-e159f3a28bb1" />

- Query results
<img width="581" height="65" alt="image" src="https://github.com/user-attachments/assets/5fea76a8-de5b-486d-a564-adfc93fa0abd" />
# Query 06: Average amount of money spent per session. Only include purchaser data in July 2017
- SQL code
<img width="1020" height="180" alt="image" src="https://github.com/user-attachments/assets/f757443e-f8a6-4801-94e3-b57120e42b11" />

- Query results
<img width="591" height="70" alt="image" src="https://github.com/user-attachments/assets/27c56178-ee9e-43e2-823a-db0b4bfcd14a" />
# Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
- SQL code
<img width="776" height="520" alt="image" src="https://github.com/user-attachments/assets/ef51f3ea-fc7a-4ff3-ae1c-74b1e562ab98" />

- Query results
<img width="485" height="708" alt="image" src="https://github.com/user-attachments/assets/3e6e2304-0a8f-4afa-a10d-8b0e6580ef4b" />

# Query 08: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. For example, 100% product view then 40% add_to_cart and 10% purchase.Add_to_cart_rate = number product  add to cart/number product view. Purchase_rate = number product purchase/number product view. The output should be calculated in product level.
- SQL code
<img width="657" height="503" alt="image" src="https://github.com/user-attachments/assets/b5c3639a-7a3f-4af3-bd3d-2feacf83bb2f" />
<img width="640" height="471" alt="image" src="https://github.com/user-attachments/assets/7329f531-3ded-4fa1-90b2-edcbafb2d89e" />
<img width="1307" height="442" alt="image" src="https://github.com/user-attachments/assets/a6499548-46e4-43b9-862d-1ba25a6d2dbc" />

- Query results
<img width="1101" height="135" alt="image" src="https://github.com/user-attachments/assets/9b0c303b-67f9-42c5-8fdd-e78292d2c9ef" />







    


