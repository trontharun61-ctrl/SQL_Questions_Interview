-- 1. Cumulative / Running Totals (Window Functions)
-- These involve SUM() OVER(), COUNT() OVER(), or other cumulative calculations.
-- Q1: Cumulative rental revenue for each store, ordered by payment date
SELECT
    s.store_id,
    p.payment_date,
    p.amount,
    SUM(p.amount) OVER (PARTITION BY s.store_id ORDER BY p.payment_date) AS cumulative_revenue
FROM payment p
JOIN staff st ON p.staff_id = st.staff_id
JOIN store s ON st.store_id = s.store_id
ORDER BY s.store_id, p.payment_date;

-- Q2: Running total of rentals per customer, ordered by rental date
select customer_id,rental_date,
count(*) over (partition by customer_id order by rental_date) as running_rentals from rental
ORDER by customer_id,rental_date;
-- Q3: Cumulative number of rentals per film over time
select i.film_id, r.rental_date, count(*) OVER (partition by i.film_id order by rental_date) as cum_total_rentals 
from rental r
LEFT join inventory i 
on r.inventory_id = i.inventory_id;
-- Q4: Total revenue collected by each staff member, maintaining a running total
select staff_id, payment_date, 
amount,
sum(amount) over (partition by staff_id order by payment_date) as running_revenue 
from payment;

-- Q5: Running total of rentals per film category, ordered by rental date
select fc.category_id,r.rental_date, count(r.rental_id) OVER (partition by fc.category_id order by r.rental_date) as running_tot_rentals
from rental r
join inventory i on r.inventory_id = i.inventory_id
JOIN film_category fc on i.film_id = fc.film_id;

-- Q22: Cumulative rental count per store, partitioned by store, ordered by rental date
select r.rental_date, s.store_id,
count(r.rental_id) over (PARTITION by s.store_id order by r.rental_date) as cumm_rentals

from rental r
join staff st on r.staff_id = st.staff_id
join store s on st.store_id = s.store_id



-- Q23: Running total of payments per customer, ordered by payment date, reset at start of year

select customer_id, payment_date,
sum(amount) over (PARTITION by customer_id, strftime('%Y', payment_date) order by payment_date) as running_total_payments
from payment



-- Q24: Cumulative rental count per film, considering customers with more than 5 rentals
with cust_morethan_5 as 
(
select customer_id,count(*) as total_rents_cust
from rental
group by 1
having count(*) > 5
)

select r.inventory_id,
count(*) over (PARTITION by r.inventory_id order by r.rental_date) as cumm_running_total_film
from rental r
join cust_morethan_5 c
on r.customer_id = c.customer_id


-- Q25: Rolling 3-month total revenue per store, excluding current month


WITH monthly_revenue AS (
    SELECT
        r.store_id,
        CAST(strftime('%Y', p.payment_date) AS INTEGER) AS revenue_year,
        CAST(strftime('%m', p.payment_date) AS INTEGER) AS revenue_month,
        SUM(p.amount) AS total_revenue
    FROM payment p
    JOIN rental r
        ON p.rental_id = r.rental_id
    GROUP BY r.store_id, revenue_year, revenue_month
)

SELECT
    store_id,
    revenue_year,
    revenue_month,
    SUM(total_revenue) OVER (
        PARTITION BY store_id
        ORDER BY revenue_year, revenue_month
        ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
    ) AS rolling_3_month_revenue
FROM monthly_revenue
ORDER BY store_id, revenue_year, revenue_month;
 
-- Q21: Cumulative revenue per customer, resetting if no rental in 3 months

WITH payments_with_lag AS (
    SELECT
        customer_id,
        payment_date,
        amount,
        LAG(payment_date) OVER (
            PARTITION BY customer_id
            ORDER BY payment_date
        ) AS prev_payment_date
    FROM payment
),
reset_flags AS (
    SELECT
        customer_id,
        payment_date,
        amount,
        CASE
            WHEN prev_payment_date IS NULL THEN 0
            WHEN julianday(payment_date) - julianday(prev_payment_date) > 90 THEN 1
            ELSE 0
        END AS reset_flag
    FROM payments_with_lag
),
reset_groups AS (
    SELECT
        customer_id,
        payment_date,
        amount,
        SUM(reset_flag) OVER (
            PARTITION BY customer_id
            ORDER BY payment_date

        ) AS reset_group
    FROM reset_flags
)
SELECT
    customer_id,
    payment_date,
    SUM(amount) OVER (
        PARTITION BY customer_id, reset_group
        ORDER BY payment_date

    ) AS cumulative_revenue
FROM reset_groups
ORDER BY customer_id, payment_date;



-- ======================================================================

                                GROUP B

-- ======================================================================


-- 2. Aggregation (SUM, AVG, COUNT, MIN, MAX, GROUP BY)
        --> These use basic aggregate functions without window partitions.

--6. Find the number of rentals made each month in the year 2005. 
SELECT strftime('%Y', rental_date) as Year, strftime('%m',rental_date) as Month, count(*) as total_rents
from rental
WHERE strftime('%Y', rental_date) = '2005'
group by Month
;

--7. Determine the average rental duration per film category, considering the rental and return dates. 
select fc.category_id,     AVG(
        julianday(r.return_date) - julianday(r.rental_date)
    ) AS avg_rental_duration_days
FROM rental r
join inventory i on r.inventory_id = i.inventory_id
JOIN film_category fc on i.film_id = fc.film_id
JOIN film f on fc.film_id = f.film_id
where return_date is NOT NULL
group by 1;


-- Q10: Total revenue per quarter for each store

SELECT
    s.store_id,
    strftime('%Y', p.payment_date) AS year,
    ((CAST(strftime('%m', p.payment_date) AS INTEGER) - 1) / 3) + 1 AS quarter,
    SUM(p.amount) AS total_revenue
FROM payment p
JOIN staff st ON p.staff_id = st.staff_id
JOIN store s ON st.store_id = s.store_id
GROUP BY s.store_id, year, quarter
ORDER BY s.store_id, year, quarter;

-- Q12: Top 3 most rented movies per store (requires aggregation + ranking)

with rentals_per_store as 
(select s.store_id,i.film_id,f.title,
count(r.rental_id) as total_rents,
dense_rank() over (PARTITION by s.store_id order by count(r.rental_id) desc) as rnk
from rental r
join inventory i on r.inventory_id = i.inventory_id
join staff st on r.staff_id = st.staff_id
join store s on st.store_id = s.store_id
join film f on i.film_id = f.film_id
group by 1,2,3
)
select store_id,film_id,title, total_rents
from rentals_per_store
where rnk <= 3;


-- Q14: Customer who rented the most movies each month
select customer_id, 
strftime('%m',rental_date) as month,count(rental_id) as total_rents,
row_number() over (PARTITION by strftime('%m',rental_date) order by count(rental_id)) as rank
from rental
group by 1,2


-- Q16: YoY percentage change in rental revenue
WITH Yearly_revenue as 
(
    SELECT 
        strftime('%Y',payment_date) as Year, count(rental_id) as total_rents, SUM(amount) as total_revenue
    FROM payment
    GROUP BY strftime('%Y',payment_date)
)
SELECT Year,total_rents,((Total_revenue - LAG(Total_revenue) OVER (ORDER BY YEAR) )/(LAG(Total_revenue) OVER (ORDER BY YEAR)) )*100 as YOy_change
from yearly_revenue


-- Q17: MoM change in rental count for the last 12 months

WITH monthly_rentals AS (
    SELECT
        strftime('%Y-%m', rental_date) AS month,
        COUNT(rental_id) AS total_rents
    FROM rental
    WHERE rental_date <= '2006-01-01'
    GROUP BY strftime('%Y-%m', rental_date)
),
mom_change AS (
    SELECT
        month,
        total_rents,
        LAG(total_rents) OVER (ORDER BY month) AS prev_month_rents,
        total_rents - LAG(total_rents) OVER (ORDER BY month) AS mom_change
    FROM monthly_rentals
)
SELECT *
FROM mom_change
ORDER BY month;

-- Q18: YoY change in number of rentals per film category
WITH rental_per_filmCategory as 
(
    SELECT 
        strftime('%Y',r.rental_date) as rental_year,
        fc.category_id,
        COUNT(r.rental_id) as total_rents
    FROM rental r 
    JOIN  inventory i 
        ON r.inventory_id = i.inventory_id
    JOIN film_category fc 
        ON i.film_id = fc.film_id
    GROUP by strftime('%Y',r.rental_date), fc.category_id
)
SELECT 
    rental_year,
    category_id, 
    total_rents,
    LAG(total_rents) OVER (PARTITION BY category_id ORDER BY rental_year) as prev_year_total_rents,
    total_rents - LAG(total_rents) OVER (PARTITION BY category_id ORDER BY rental_year) as yoy_change_total
FROM rental_per_filmCategory;

-- Q19: Compare monthly revenue for same month across different years
WITH revenue_year_month as 
(
    SELECT 
        strftime('%Y',payment_date) as payment_year, 
        strftime('%m',payment_date) as payment_month, 
        sum(amount) as total_revenue
    FROM payment p
    GROUP BY strftime('%Y-%m',payment_date),strftime('%m',payment_month)
) 
SELECT 
    payment_year, 
    payment_month,
    total_revenue, 
    total_revenue - LAG(total_revenue) OVER (PARTITION BY payment_month ORDER BY payment_year) as prev_month_revenue_change
FROM revenue_year_month;


-- Q20: Difference in rental revenue between current and previous month per store
WITH month_rental_revenue as 
(
    SELECT 
        s.store_id, 
        strftime('%m',r.rental_date) as rental_month,
        strftime('%Y',r.rental_date) as rental_year,
        count(r.rental_id) as total_rents,
        sum(p.amount) as rental_revenue
    FROM rental r
    JOIN staff st 
        ON r.staff_id = st.staff_id
    JOIN payment p
        ON r.rental_id = p.rental_id
    JOIN store s
        ON st.store_id = s.store_id
    GROUP by  s.store_id, strftime('%m',r.rental_date),strftime('%Y',r.rental_date) 
)

SELECT store_id, rental_month,rental_year,total_rents,rental_revenue, 
LAG(rental_revenue) OVER (PARTITION BY store_id,rental_year  ORDER BY rental_year) as lag_revenue
FROM month_rental_revenue


-- Q26: Customer who rented movies for the longest total duration
SELECT 
    r.customer_id,c.first_name,c.last_name,
    CAST(sum(julianday(return_date) - julianday(rental_date)) as INTEGER)  as rented_duration 

FROM rental r
JOIN customer c
    ON r.customer_id = c.customer_id
GROUP BY r.customer_id,c.first_name,c.last_name
ORDER BY rented_duration
LIMIT 1


-- Q27: Difference in days between first and last rental per customer
WITH diff_days as 
(
    SELECT 
        customer_id, 
        min(rental_date) as first_rental_date, 
        max(rental_date) as last_rental_date
    
    FROM rental
    GROUP BY customer_id
)
SELECT 
    customer_id,
    CAST((julianday(last_rental_date) - julianday(first_rental_date)) as INTERGER)  as days_diff
FROM diff_days
ORDER BY 2 DESC


-- Q28: Month with highest rental revenue for each store, considering only years with revenue growth

WITH yearly_revenue as 
(
    SELECT 
        s.store_id, 
        strftime('%Y', p.payment_date) as payment_year, 
        sum(p.amount) as rental_revenue
    FROM rental r 
    JOIN payment p
        ON r.rental_id = P.rental_id
    JOIN staff st
        ON p.staff_id = st.staff_id
    JOIN store s 
        ON st.store_id = s.store_id
    GROUP BY s.store_id, strftime('%m', p.payment_date) 
),

yearly_growth as 
(
    SELECT 
        store_id, 
        payment_year, 
        rental_revenue, 
        lag(rental_revenue) OVER (PARTITION BY store_id ORDER BY payment_year) as prev_year_revenue
    FROM yearly_revenue

),
yearly_rev_growth as 
(
    SELECT store_id, payment_year
    FROM yearly_growth
    WHERE prev_year_revenue IS NOT NULL 
    AND rental_revenue > prev_year_revenue
), 
monthly_revenue AS (
    SELECT



-- Q30: Total revenue per quarter and quarter with highest increase vs previous quarter
WITH quarterly_revenue AS (
    SELECT
        CAST(strftime('%Y', payment_date) AS INTEGER) AS revenue_year,
        CAST(strftime('%m', payment_date) AS INTEGER) AS revenue_month,
        ( (CAST(strftime('%m', payment_date) AS INTEGER) - 1) / 3 + 1 ) AS revenue_quarter,
        SUM(amount) AS total_revenue
    FROM payment
    GROUP BY revenue_year, revenue_quarter
),

quarterly_diff AS (
    SELECT
        revenue_year,
        revenue_quarter,
        total_revenue,
        total_revenue - LAG(total_revenue) OVER (
            ORDER BY revenue_year, revenue_quarter
        ) AS revenue_change
    FROM quarterly_revenue
)

SELECT
    revenue_year,
    revenue_quarter,
    total_revenue,
    revenue_change
FROM quarterly_diff
ORDER BY revenue_change DESC
LIMIT 1; 

-- Q35: Rolling 6-month average rental count for each category
WITH rental_count_month as 
(
    SELECT 
        fc.category_id, 
        strftime('%Y-%m', rental_date) as year_month,
        count(r.rental_id) as rental_count
    FROM rental r
    JOIN inventory i 
        ON r.inventory_id = i.inventory_id
    JOIN film_category fc 
        ON i.film_id = fc.film_id
    GROUP BY fc.category_id,strftime('%Y-%m', rental_date)
),
row_numbers as 
(
SELECT 
    category_id,
    year_month, 
    rental_count, 
    ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY year_month) as rn 
FROM rental_count_month
)

SELECT category_id, year_month,rental_count,
    AVG(rental_count) OVER (PARTITION BY category_id ORDER BY rn
    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) as rolling_avg
FROM row_numbers



-- Q36: YoY % change in number of rentals per store, find stores with highest growth

WITH store_rentals AS (
    SELECT
        s.store_id,
        CAST(strftime('%Y', r.rental_date) AS INTEGER) AS rental_year,
        COUNT(*) AS rental_count
    FROM rental r
    JOIN staff st   
        ON r.staff_id = st.staff_id
    JOIN store s 
        ON st.store_id = s.store_id
    GROUP BY s.store_id, rental_year
),

store_rentals_yoy AS (
    SELECT
        store_id,
        rental_year,
        rental_count,
        LAG(rental_count) OVER (
            PARTITION BY store_id
            ORDER BY rental_year
        ) AS prev_year_count
    FROM store_rentals
)

SELECT
    store_id,
    rental_year,
    rental_count,
    prev_year_count,
    CASE
        WHEN prev_year_count IS NULL THEN NULL
        ELSE ROUND( (rental_count - prev_year_count) * 100.0 / prev_year_count, 2)
    END AS yoy_pct_change
FROM store_rentals_yoy
ORDER BY yoy_pct_change DESC


-- Q37: MoM change in total revenue per staff member, ordered by payment date

WITH staff_revenue as 
(
    SELECT 
        st.staff_id,
        --p.payment_date,
        strftime('%Y', payment_date) as payment_year,
        strftime('%m', payment_date) as payment_month,
        sum(amount) as total_revenue 
    FROM payment p 
    JOIN staff st 
        ON p.staff_id = st.staff_id
    GROUP BY st.staff_id, 
    --p.payment_date, 
    strftime('%Y-%m', payment_date)
)
SELECT 
    staff_id, 
    payment_year,
    payment_month, 
    total_revenue, 
    LAG(total_revenue) OVER (PARTITION BY staff_id ORDER BY payment_year, payment_month) as lag_rental_revenue,
    total_revenue - LAG(total_revenue) OVER (PARTITION BY staff_id ORDER BY payment_year, payment_month) as change
FROM staff_revenue
;

-- Q38: YoY change in average rental duration per category
WITH rental_duration AS (
    SELECT 
        fc.category_id,
        CAST(strftime('%Y', r.rental_date) AS INTEGER) AS rental_year,
        julianday(r.return_date) - julianday(r.rental_date) AS days_diff
    FROM rental r
    JOIN inventory i
        ON r.inventory_id = i.inventory_id
    JOIN film_category fc
        ON i.film_id = fc.film_id
   -- WHERE r.return_date IS NOT NULL
),

avg_rental_duration_per_year AS (
    SELECT 
        category_id,
        rental_year,
        AVG(days_diff) AS avg_rental_duration
    FROM rental_duration
    GROUP BY category_id, rental_year
)

SELECT 
    category_id,
    rental_year,
    avg_rental_duration,
    avg_rental_duration - LAG(avg_rental_duration)
          OVER (PARTITION BY category_id ORDER BY rental_year) AS yoy_change
FROM avg_rental_duration_per_year
ORDER BY category_id, rental_year;

-- Q39: Compare monthly rental count for same month across years, find month with biggest drop

WITH monthly_rentals AS 
(
    SELECT 
        CAST(strftime('%Y', rental_date) AS INTEGER) AS rental_year,
        CAST(strftime('%m', rental_date) AS INTEGER) AS rental_month,
        COUNT(*) AS rental_count
    FROM rental
    GROUP BY rental_year, rental_month
),

monthly_diff AS (
    SELECT
        rental_month,
        rental_year,
        rental_count,
        rental_count - LAG(rental_count) OVER (
            PARTITION BY rental_month
            ORDER BY rental_year
        ) AS diff_from_prev_year
    FROM monthly_rentals
)

SELECT
    rental_month,
    rental_year,
    rental_count,
    diff_from_prev_year
FROM monthly_diff
ORDER BY diff_from_prev_year ASC
LIMIT 1;


-- Q40: Difference in customer spending compared to previous year (only for recurring customers)

WITH recurring_customers as 
(
    SELECT customer_id
    FROM payment
    GROUP BY customer_id
    HAVING count(strftime('%Y',payment_date)) > 1
), 
cust_spend as 
(
    SELECT  strftime('%Y', p.payment_date) as payment_year, 
    sum(p.amount) as amount_spent
    FROM payment p 
    JOIN recurring_customers rc 
        ON p.customer_id = rc.customer_id
    GROUP BY strftime('%Y',p.payment_date) 
    
)

SELECT payment_year, amount_spent- LAG(amount_spent) OVER (ORDER BY payment_year) as change
FROM cust_spend


-- Q43: Average revenue per customer per film category, rank customers within category

WITH avg_revenue as 
(
SELECT 
    fc.category_id,
    r.customer_id,  
    avg(p.amount) as revenue

FROM payment p
JOIN rental r 
    ON p.rental_id = r.rental_id
JOIN inventory i 
    ON r.inventory_id = i.inventory_id
JOIN film_category fc 
    ON i.film_id = fc.film_id
GROUP BY fc.category_id, r.customer_id
)

SELECT 
    category_id, 
    customer_id, 
    revenue,
    RANK() OVER (PARTITION BY category_id ORDER BY revenue DESC) as rank 
FROM avg_revenue



-- Q44: Customers who rented more in last 6 months than previous 6 months

WITH rental_counts AS (
    SELECT
        customer_id,
        SUM(
            CASE 
                WHEN rental_date >= DATE('now', '-6 months') 
                THEN 1 ELSE 0 
            END
        ) AS last_6_months,
        SUM(
            CASE 
                WHEN rental_date >= DATE('now', '-12 months')
                 AND rental_date <  DATE('now', '-6 months')
                THEN 1 ELSE 0 
            END
        ) AS prev_6_months
    FROM rental
    GROUP BY customer_id
)

SELECT
    customer_id,
    last_6_months,
    prev_6_months
FROM rental_counts
WHERE last_6_months > prev_6_months
ORDER BY customer_id;



-- Q68: Total revenue by movies released in each decade, broken down by category

SELECT 
    ((CAST(strftime('%Y', p.payment_date) as INTEGER))/10 ) * 10 as payment_year, 
    fc.category_id, 
    sum(p.amount) as revenue

FROM payment p 
JOIN rental r 
    ON p.rental_id = r.rental_id
JOIN inventory i 
    ON r.inventory_id = i.inventory_id
JOIN film f 
    ON i.film_id = f.film_id
JOIN film_category fc 
    ON f.film_id = fc.film_id

GROUP BY 1,2 

-- Q70: Store with highest number of unique customers who rented at least 10 times
WITH cus_rental as 
(
    SELECT 
        s.store_id, 
        r.customer_id, 
        count(r.rental_id) as rental_count
    FROM rental r 
    JOIN staff st 
        ON r.staff_id = st.staff_id
    JOIN store s
        ON st.store_id = s.store_id 
    GROUP BY s.store_id,r.customer_id
    HAVING count(r.rental_id) >= 10
) 
SELECT 
    store_id, 
    count(DISTINCT customer_id) as cust_count
FROM cus_rental
ORDER BY cust_count DESC 
LIMIT 1



-- Q114: Month generating highest rental revenue per store
WITH month_revenue as 
(
    SELECT 
        s.store_id,
        strftime('%Y-%m', payment_date) as rental_month, 
        sum(p.amount) as rental_revenue
    FROM payment p 
    JOIN staff st 
        ON p.staff_id = st.staff_id
    JOIN store s
        ON st.store_id = s.store_id 
    GROUP BY s.store_id, strftime('%m',payment_date)
),

highest_revenue as 
(
    SELECT store_id,
        rental_month, 
        rental_revenue, 
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY rental_revenue DESC) as rank 
    FROM Month_revenue
) 
SELECT store_id, 
    rental_month, 
    rental_revenue
FROM highest_revenue
WHERE rank = 1
ORDER BY store_id




-- Q115: Highest and lowest revenue-generating months per category

WITH month_revenue as 
(
    SELECT 
        fc.category_id,
        strftime('%Y-%m', payment_date) as payment_month, 
        sum(p.amount) as rental_revenue
    FROM payment p 
    JOIN rental r 
        ON p.rental_id = r.rental_id
    JOIN inventory i 
        ON r.inventory_id = i.inventory_id
    JOIN film_category fc 
        ON i.film_id = fc.film_id
    
    GROUP BY fc.category_id, strftime('%Y-%m',payment_date)
),

highest_lowest_revenue as 
(
    SELECT category_id,
        payment_month, 
        rental_revenue, 
        ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY rental_revenue DESC) as higheset_rank,
        ROW_number() OVER (PARTITION BY category_id ORDER BY rental_revenue ASC) as lowest_rank 
    FROM Month_revenue
) 
SELECT category_id, 
    payment_month, 
    rental_revenue, 
    higheset_rank, 
    lowest_rank
FROM highest_lowest_revenue
WHERE (higheset_rank = 1 OR lowest_rank = 1)
ORDER BY category_id
;



-- ========================================

                 GROUP - C 

-- ========================================

-- 3. Ranking / Top-N Queries (RANK, DENSE_RANK, NTILE)

-- Q11: Rank movies by total rental count (highest first)
WITH rental_count_by_movie as 
(
    SELECT 
        f.film_id, 
        f.title, 
        count(r.rental_id) as rental_count
        
    FROM rental r
    JOIN inventory i
        ON r.inventory_id = i.inventory_id
    JOIN film f 
        ON i.film_id = f.film_id
    GROUP BY f.film_id, f.title
    ORDER BY rental_count DESC
)
SELECT 
    film_id, title, rental_count,
    DENSE_RANK() OVER (ORDER BY rental_count DESC) as rank 
FROM rental_count_by_movie

-- Q12: Top 3 most rented movies per store
WITH rented_count_store as 
(
    SELECT
        s.store_id,
        f.film_id,
        f.title,
        count(r.rental_id) as rental_count, 
        DENSE_RANK() OVER (PARTITION BY s.store_id ORDER BY count(r.rental_id) DESC) as Rank 
    FROM rental r
    JOIN inventory i 
        ON r.inventory_id = i.inventory_id
    JOIN film f
        ON i.film_id = f.film_id
    JOIN store s 
        ON i.store_id = s.store_id
    GROUP BY s.store_id, f.film_id, f.title
) 
SELECT store_id, 
    film_id, 
    title, 
    rental_count, rank
FROM rented_count_store
WHERE rank <= 3


-- Q13: Rank customers by total spending per store
WITH cust_spends as 
(
    SELECT 
        s.store_id, 
        r.customer_id, 
        sum(p.amount) as cust_spent_amount
    FROM payment p 
    JOIN rental r 
        ON p.rental_id = r.rental_id 
    JOIN staff st
        ON r.staff_id = st.staff_id
    JOIN store s 
        ON st.store_id = s.store_id
    GROUP BY s.store_id, r.customer_id
)
SELECT store_id, 
customer_id, 
cust_spent_amount, 
RANK() OVER (PARTITION BY store_id ORDER BY cust_spent_amount DESC) as rank 
FROM cust_spends

-- Q31: Rank customers within their store based on total rental payments, considering ties
WITH cust_spends as 
(
    SELECT 
        s.store_id, 
        r.customer_id, 
        sum(p.amount) as cust_spent_amount
    FROM payment p 
    JOIN rental r 
        ON p.rental_id = r.rental_id 
    JOIN staff st
        ON r.staff_id = st.staff_id
    JOIN store s 
        ON st.store_id = s.store_id
    GROUP BY s.store_id, r.customer_id
)
SELECT store_id, 
customer_id, 
cust_spent_amount, 
DENSE_RANK() OVER (PARTITION BY store_id ORDER BY cust_spent_amount DESC) as rank 
FROM cust_spends

-- Q32: Top 3 rented movies per category, dense ranking

WITH cat_rents as 
(
    SELECT   
        f.film_id, 
        f.title, 
        fc.category_id,
        COUNT(r.rental_id) as rental_count
    FROM rental r 
    JOIN inventory i 
        ON r.inventory_id = i.inventory_id
    JOIN film f
        ON i.film_id = f.film_id
    JOIN film_category fc 
        ON f.film_id = fc.film_id
    GROUP BY f.film_id, f.title, fc.category_id 
), 
rank_movies as 
(
    SELECT 
        film_id, 
        title, 
        category_id, rental_count, 
        DENSE_RANK() OVER (PARTITION BY category_id ORDER BY rental_count DESC) as Rank 
    FROM cat_rents
) 
select film_id, 
title, 
category_id, rental_count
FROM rank_movies 
WHERE rank <=3

-- Q33: Rank customers by total spending, excluding one-time renters
WITH customers_one as 
(

    SELECT 
        customer_id
    FROM payment 
    GROUP BY customer_id
    HAVING count(payment_date) > 1

),
total_spend as 
(
    SELECT p.customer_id,
        sum(p.amount) as total_spend,
        RANK() OVER (ORDER BY sum(p.amount) DESC) as rank 
    
    FROM payment p
    JOIN customers_one c
        ON p.customer_id = c.customer_id
    GROUP BY p.customer_id
)
SELECT customer_id, total_spend, rank
FROM total_spend



-- Q34: Most rented movie per month, ranking by rental count
WITH rented_movies as 
(
    SELECT strftime('%Y-%m', r.rental_date) as rental_year_month, 
    f.film_id,
    f.title, 
    count(r.rental_id) as total_rent
    -- RANK() OVER(PARTITION BY strftime('%Y-%m', r.rental_date) ORDER BY count(r.rental_id) DESC) as rank 
    FROM rental r
    JOIN inventory i 
        ON r.inventory_id = i.inventory_id
    JOIN film f 
        ON i.film_id = f.film_id
    GROUP BY strftime('%Y-%m', r.rental_date), f.film_id, f.title
), 
rents as 
(
    SELECT rental_year_month, 
    film_id, 
    title, 
    total_rent, 
    RANK() OVER (PARTITION BY rental_year_month ORDER BY total_rent DESC) as RANK 
    FROM rented_movies
)
SELECT rental_year_month, 
film_id, 
title, 
total_rent 
FROM rents 
WHERE RANK =1 


-- Q74: Rank films by rental count per category using DENSE_RANK
WITH cat_rents as 
(
    SELECT   
        f.film_id, 
        f.title, 
        fc.category_id,
        COUNT(r.rental_id) as rental_count
    FROM rental r 
    JOIN inventory i 
        ON r.inventory_id = i.inventory_id
    JOIN film f
        ON i.film_id = f.film_id
    JOIN film_category fc 
        ON f.film_id = fc.film_id
    GROUP BY f.film_id, f.title, fc.category_id 
) 
SELECT 
    film_id, 
    title, 
    category_id, rental_count, 
    DENSE_RANK() OVER (PARTITION BY category_id ORDER BY rental_count DESC) as Rank 
FROM cat_rents

-- Q75: Top 3 customers with highest rental payments per store using RANK
WITH cust_spends as 
(
    SELECT 
        s.store_id, 
        r.customer_id, 
        sum(p.amount) as cust_spent_amount,
        RANK() OVER (PARTITION BY s.store_id ORDER BY sum(p.amount) DESC) as rank 
    FROM payment p 
    JOIN rental r 
        ON p.rental_id = r.rental_id 
    JOIN staff st
        ON r.staff_id = st.staff_id
    JOIN store s 
        ON st.store_id = s.store_id
    GROUP BY s.store_id, r.customer_id
)
SELECT store_id, 
customer_id, 
cust_spent_amount,
rank
FROM cust_spends
wHERE rank <=3

-- Q76: Staff member with most rentals per month, ranked using DENSE_RANK
WITH staff_rents as 
(
    SELECT 
        st.staff_id, 
        first_name || ' ' || last_name as staff_name,
        strftime('%Y-%m',r.rental_date) as rental_year_month,
        count(rental_id) as rental_count
    FROM rental r
    JOIN staff st
        ON r.staff_id = st.staff_id
    GROUP BY st.staff_id, first_name || ' ' || last_name,  strftime('%Y-%m',r.rental_date) 
) 
SELECT 
    staff_id, staff_name, 
    rental_year_month, 
    rental_count, 
    DENSE_RANK() OVER (PARTITION BY rental_year_month ORDER BY rental_count DESC) as rank 
FROM staff_rents
ORDER BY rental_year_month, rank


-- Q78: Assign each rental to a quartile (NTILE(4)) based on payment amount

SELECT
    p.payment_id,
    p.rental_id,
    p.customer_id,
    p.amount,
    NTILE(4) OVER (
        ORDER BY p.amount DESC
    ) AS payment_quartile
FROM payment p
ORDER BY p.amount DESC;

-- Q79: Most rented movie per month using RANK() OVER(PARTITION BY month)

WITH monthly_rentals AS (
    SELECT
        strftime('%Y-%m', r.rental_date) AS rental_month,
        f.film_id,
        f.title,
        COUNT(r.rental_id) AS rental_count
    FROM rental r
    JOIN inventory i
        ON r.inventory_id = i.inventory_id
    JOIN film f
        ON i.film_id = f.film_id
    GROUP BY rental_month, f.film_id, f.title
),
ranked_movies AS (
    SELECT
        rental_month,
        film_id,
        title,
        rental_count,
        RANK() OVER (
            PARTITION BY rental_month
            ORDER BY rental_count DESC
        ) AS rank
    FROM monthly_rentals
)
SELECT
    rental_month,
    film_id,
    title,
    rental_count,
    rank
FROM ranked_movies
WHERE rank = 1
ORDER BY rental_month;



-- Q89: Rank customers within each store based on total payments (ties considered)
WITH cust_store as 
(
SELECT 
    
    s.store_id,
    p.customer_id, 
    sum(p.amount) as total_payment
FROM payment p 
JOIN staff st 
    ON p.staff_id = st.staff_id
JOIN store s 
    ON st.store_id = s.store_id
GROUP BY s.store_id,p.customer_id
) 
SELECT store_id, 
customer_id, 
total_payment, 
DENSE_RANK() OVER(PARTITION BY store_id ORDER BY total_payment DESC) as rank 
FROM cust_store


-- Q90: Top 3 most frequent renters in each store

WITH customer_rentals AS (
    SELECT
        i.store_id,
        r.customer_id,
        COUNT(r.rental_id) AS rental_count
    FROM rental r
    JOIN inventory i
        ON r.inventory_id = i.inventory_id
    GROUP BY i.store_id, r.customer_id
),
ranked_customers AS (
    SELECT
        store_id,
        customer_id,
        rental_count,
        DENSE_RANK() OVER (
            PARTITION BY store_id
            ORDER BY rental_count DESC
        ) AS rank
    FROM customer_rentals
)
SELECT
    store_id,
    customer_id,
    rental_count
FROM ranked_customers
WHERE rank <= 3
ORDER BY store_id, rental_count DESC;



-- Q96: Most rented movie per store, compared with second-most rented
WITH movie_rentals AS (
    SELECT
        i.store_id,
        f.film_id,
        f.title,
        COUNT(r.rental_id) AS rental_count
    FROM rental r
    JOIN inventory i
        ON r.inventory_id = i.inventory_id
    JOIN film f
        ON i.film_id = f.film_id
    GROUP BY i.store_id, f.film_id, f.title
),
ranked_movies AS (
    SELECT
        store_id,
        film_id,
        title,
        rental_count,
        DENSE_RANK() OVER (
            PARTITION BY store_id
            ORDER BY rental_count DESC
        ) AS rank
    FROM movie_rentals
)
SELECT
    m1.store_id,
    m1.film_id   AS top_movie_id,
    m1.title     AS top_movie_title,
    m1.rental_count AS top_rentals,
    m2.film_id   AS second_movie_id,
    m2.title     AS second_movie_title,
    m2.rental_count AS second_rentals,
    (m1.rental_count - m2.rental_count) AS rental_difference
FROM ranked_movies m1
LEFT JOIN ranked_movies m2
    ON m1.store_id = m2.store_id
   AND m2.rank = 2
WHERE m1.rank = 1
ORDER BY m1.store_id;


-- ==========================

            GROUP - D 
-- 4. Date/Time Analysis (DATE functions, LAG, LEAD, Gaps)
-- ==========================




-- Q9: Day of the week with highest number of rentals


-- Q71: Previous and next rental date per customer using LAG and LEAD

SELECT 
    customer_id, 
    rental_date, 
    LAG(rental_date) OVER(PARTITION BY customer_id ORDER BY rental_date) as prev_rental_date, 
    LEAD(rental_date) OVER(PARTITION BY customer_id ORDER BY rental_date) as next_rental_date
FROM rental 

-- Q72: Time difference (days) between each rental for a customer using LAG

SELECT 
    customer_id, 
    rental_date, 
    LAG(rental_date) OVER(PARTITION BY customer_id ORDER BY rental_date) as prev_rental_date,
    ROUND((julianday(rental_date) -  julianday(LAG(rental_date) OVER(PARTITION BY customer_id ORDER BY rental_date))),2) as diff
FROM rental 


-- Q73: Rental revenue trend per store, previous and next month using LEAD/LAG


-- Q80: Customers who haven’t rented in last 3 months using LAG


-- Q86: Previous and next rental dates per customer, with difference in days
SELECT customer_id, 
rental_date, prev_rental_date, next_rental_date, 
-- Days since previous rental
    CASE 
        WHEN prev_rental_date IS NOT NULL 
        THEN CAST((julianday(rental_date) - julianday(prev_rental_date)) AS INTEGER) 
    END AS days_since_prev,
    -- Days until next rental
    CASE 
        WHEN next_rental_date IS NOT NULL 
        THEN CAST( (julianday(next_rental_date) - julianday(rental_date)) AS INTEGER) 
    END AS days_until_next
FROM(
    SELECT 
        customer_id, 
        rental_date, 
        LAG(rental_date) OVER(PARTITION BY customer_id ORDER BY rental_date) as prev_rental_date, 
        LEAD(rental_date) OVER(PARTITION BY customer_id ORDER BY rental_date) as next_rental_date

    FROM rental
    )

-- Q87: Customers with gap >30 days between rentals
WITH diffIn_rentals as 
(
SELECT customer_id, 
rental_date, prev_rental_date, next_rental_date, 
-- Days since previous rental
    CASE 
        WHEN prev_rental_date IS NOT NULL 
        THEN CAST((julianday(rental_date) - julianday(prev_rental_date)) AS INTEGER) 
    END AS days_since_prev,
    -- Days until next rental
    CASE 
        WHEN next_rental_date IS NOT NULL 
        THEN CAST( (julianday(next_rental_date) - julianday(rental_date)) AS INTEGER) 
    END AS days_until_next
FROM(
    SELECT 
        customer_id, 
        rental_date, 
        LAG(rental_date) OVER(PARTITION BY customer_id ORDER BY rental_date) as prev_rental_date, 
        LEAD(rental_date) OVER(PARTITION BY customer_id ORDER BY rental_date) as next_rental_date

    FROM rental
    )
) 

SELECT customer_id, 
rental_date, prev_rental_date, days_since_prev as gap
FROM 
diffIn_rentals
WHERE days_since_prev > 30
ORDER BY days_since_prev;

-- Q88: Most recent and second-most recent rental per customer


-- Q95: Customers who rented in three consecutive months




-- Q98: Movie with highest difference in rental count between consecutive months


-- Q103: Rental revenue change per movie compared to previous quarter


 -- GROUP - E 



-- 4. Date/Time Analysis (DATE functions, LAG, LEAD, Gaps)
-- Q9: Day of the week with highest number of rentals
-- Q71: Previous and next rental date per customer using LAG and LEAD
-- Q72: Time difference (days) between each rental for a customer using LAG
-- Q73: Rental revenue trend per store, previous and next month using LEAD/LAG
-- Q80: Customers who haven’t rented in last 3 months using LAG
-- Q86: Previous and next rental dates per customer, with difference in days
-- Q87: Customers with gap >30 days between rentals
-- Q88: Most recent and second-most recent rental per customer
-- Q95: Customers who rented in three consecutive months
-- Q98: Movie with highest difference in rental count between consecutive months
-- Q103: Rental revenue change per movie compared to previous quarter



-- 5. Join / Multi-Table Queries
-- Q8: Customers who rented in same month across multiple years
-- Q41: Customers who rented from every category at least once
-- Q42: Actors whose movies generate highest revenue (movies rented >50 times)
-- Q46: Customers who rented movies from both stores, total rentals per customer
-- Q48: Customers who rented movies in ≥3 categories, count rentals per category
-- Q49: Customers who never rented from their original store
-- Q50: Customers who rented from only one store
-- Q51: Movies in inventory but never rented from that store
-- Q52: Movies rented most but with least inventory per store
-- Q53: Availability of each film per store, show store with most copies
-- Q54: Films returned late most, include category and actor
-- Q55: Category with lowest return rate (rental vs return count)
-- Q56: Top 5 most rented movies per actor
-- Q57: Actors whose movies generated most rental revenue
-- Q58: Actor appearing in most rented films
-- Q59: Actors whose movies rented least in past year, list movies
-- Q60: Actors in movies belonging to most categories



-- 6. Staff / Store Analysis
-- Q61: Staff with highest revenue in total rentals
-- Q62: Total revenue per staff, broken down by store
-- Q63: Staff handling most rentals during peak hours
-- Q64: Store with highest % revenue from repeat customers
-- Q65: Store with highest late fee collection
-- Q66: Customers renting movies featuring same actor more than once
-- Q67: Customers renting movies from every category in a particular store
-- Q106: Staff member with highest revenue contribution per store
-- Q107: Rentals handled per staff per month, compare to previous month
-- Q108: Staff member with longest gap between rentals
-- Q110: Staff with highest number of late rentals, compare to second-highest



-- 7. Analytical / Trend Calculations (YoY, MoM, Rolling Avg)
-- Q16: YoY % change in rental revenue
-- Q17: MoM change in rental count last 12 months
-- Q18: YoY change in number of rentals per category
-- Q25: Rolling 3-month total revenue per store, excluding current month
-- Q35: Rolling 6-month average rental count per category
-- Q36: YoY % change in rentals per store, find highest growth
-- Q37: MoM change in revenue per staff
WITH staff_revenue as 
(
    SELECT
        p.staff_id,
        strftime('%Y-%m', p.payment_date) as payment_year_month,
        sum(p.amount) as revenue
    FROM payment p 
    JOIN staff st 
        ON p.staff_id = st.staff_id
    GROUP BY p.staff_id, strftime('%Y-%m', p.payment_date)
)

SELECT 
    staff_id,
    payment_year_month,
    revenue,
    LAG(revenue) OVER (PARTITION BY staff_id ORDER BY payment_year_month) as prev_mon_revenue,
    COALESCE((revenue - LAG(revenue) OVER (PARTITION BY staff_id ORDER BY payment_year_month)),0) as mom_change
FROM staff_revenue
-- Q38: YoY change in average rental duration per category
-- Q39: Monthly rental comparison across years, biggest drop
-- Q40: Customer spending difference compared to previous year
-- Q73: Revenue trend per store, previous & next month



-- 8. Optimization / Index Analysis
-- Q81: Identify queries benefiting from index based on common searches
-- Q82: Queries optimized by indexing rental_date
-- Q83: Performance of searching by last name before/after index
-- Q84: Most frequently rented movies, test inventory_id index performance
-- Q85: Indexing payment_date effect on monthly revenue calculations



-- 9. Customer Behavior / Loyalty Analysis
-- Q91: Classify customers by loyalty: Premium, Regular, Low-spending
-- Q92: Customers who spent more last 6 months than previous 6 months
-- Q93: Customers renting same film multiple times, time between rentals
-- Q94: First and most recent rental per customer
-- Q97: Avg rental duration per movie, only if rented >20 times
-- Q100: Previous and next rental instance per film per store
-- Q101: Movies with high rentals but low inventory
-- Q102: Most popular film category per month
-- Q104: Films rented every month in a given year
-- Q105: Store with highest % rentals from first-time customers

-- 10. Advanced Analysis / Complex Metrics
-- Q29: Movies most frequently rented on Fridays & Saturdays
-- Q44: Customers renting more in last 6 months than previous 6 months
-- Q45: Store with highest % unique customers renting only once
-- Q47: Top 3 customers per store by total rental payments



-- Q72: Time difference (days) between each rental for a customer
SELECT 
    customer_id, 
    rental_date, 
    LAG(rental_date) OVER(PARTITION BY customer_id ORDER BY rental_date) as prev_rental_date,
    ROUND((julianday(rental_date) -  julianday(LAG(rental_date) OVER(PARTITION BY customer_id ORDER BY rental_date))),2) as diff
FROM rental 

-- Q99: Films frequently rented together by same customer
