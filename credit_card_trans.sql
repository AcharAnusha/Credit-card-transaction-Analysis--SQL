-- Create database
create database credit_card_transactions;

use credit_card_transactions;

-- Create Transactions table in database
Create table transactions(id int primary key, city VARCHAR(50), 
date VARCHAR(50), card_type VARCHAR(50), exp_type VARCHAR(50),
gender VARCHAR(50), amount int);

-- Load data into table
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Credit card transactions.csv'
INTO TABLE transactions
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- View the table
select * from transactions;

-- Find no. of records
select count(*) from transactions;

-- 1. Find top 5 cities with highest spends and their percentage contribution of total credit card spends

select city, sum(amount) as city_spend, 
round(sum(amount)/(select sum(amount) from transactions) *100, 2) as total_spend_perc
from transactions 
group by city 
order by city_spend desc 
limit 5;

-- 2. Query to print highest spend month and amount spent in that month for each card type

with cte_1 as
(select MONTH(STR_TO_DATE(date, '%d-%b-%y')) as mnth, YEAR(STR_TO_DATE(date, '%d-%b-%y')) as yr, 
card_type, sum(amount) as mnth_spend from transactions 
group by mnth, yr, card_type
order by mnth_spend desc),
cte_2 as 
(select *, RANK() over (partition by card_type order by mnth_spend desc) as rnk
from cte_1)
select card_type, mnth, yr, mnth_spend from cte_2
where rnk=1;

-- 3. Write a query to print the transaction details for each card type when it reaches a
-- cumulative of 1000000 total spends

with cte1 as 
(select *, SUM(amount) over (partition by card_type order by date, id) as cumm_spend 
from transactions),
cte2 as
(select *, ROW_NUMBER() OVER (partition by card_type order by cumm_spend) as rnk 
from cte1
where cumm_spend>=1000000)
select id, city, date, card_type, exp_type, gender, amount, cumm_spend from cte2 where rnk=1;

-- 4. Find which city has lowest percentage spend for gold card type

select city, sum(amount) as city_spend, 
round(sum(amount)*100.0/ (select sum(amount) from transactions where card_type='Gold'),2) as perc_spend 
from transactions 
where card_type='Gold' 
group by city 
order by perc_spend;

-- 5. Query to find highest and lowest expense type for each city

with cte1 as
(select city, exp_type, sum(amount) as exp_amount from transactions
group by city, exp_type),
cte2 as
(select *, 
rank() over (partition by city order by exp_amount) as lowest_spend_rnk,
rank() over (partition by city order by exp_amount desc) as highest_spend_rnk
from cte1)
select city, max(case when lowest_spend_rnk=1 then exp_type end) as lowest_exp_type,
max(case when highest_spend_rnk=1 then exp_type end) as highest_exp_type
from cte2 
group by city;

-- 6. Percentage contribution of spends by females for each expense type

select exp_type, 
round((sum(case when gender='F' then amount end)/sum(amount))*100,2) as female_percent_contribution
from transactions 
group by exp_type;

-- 7. Calculate the Month-over-Month (MoM) percentage growth in total spending for each card type

WITH monthly_spend AS 
(select card_type,
YEAR(str_to_date(date,"%d-%b-%y")) AS yr,
MONTH(str_to_date(date,"%d-%b-%y")) AS mn,
SUM(amount) AS total_spend
FROM transactions
GROUP BY card_type, yr,mn
)
SELECT card_type, yr, mn, total_spend,
round((total_spend - LAG(total_spend) OVER (PARTITION BY card_type ORDER BY yr, mn))*100.0/ 
LAG(total_spend) OVER (PARTITION BY card_type ORDER BY yr, mn), 2) AS mom_growth_percent
FROM monthly_spend;

-- 8. During weekend, which city has highest spend to transactions ratio

select city, sum(amount)/count(id) as spend_to_trans_ratio
from transactions 
where DAYNAME(str_to_date(date,"%d-%b-%y")) in ("Saturday", "Sunday") 
group by city 
order by spend_to_trans_ratio desc 
limit 1;

-- 9. Which city took the least no. of days to reach its 500th transaction

with cte1 as
(select *, 
row_number() over (partition by city order by str_to_date(date,"%d-%b-%y")) as rnk, 
min(str_to_date(date,"%d-%b-%y")) over (order by str_to_date(date,"%d-%b-%y")) as min_date
from transactions)
select city, datediff(str_to_date(date, "%d-%b-%y"), min_date) as days_to_500
from cte1 where rnk=500 
order by days_to_500 limit 1;
