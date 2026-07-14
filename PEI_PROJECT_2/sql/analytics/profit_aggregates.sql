
-- ==========================================================
-- Profit By Year
-- ==========================================================

SELECT

    YEAR(order_date) AS order_year,

    ROUND(SUM(profit), 2) AS total_profit

FROM ecommerce_dev.gold.sales_master

GROUP BY

    YEAR(order_date)

ORDER BY

    order_year;



-- ==========================================================
-- Profit By Year + Product Category
-- ==========================================================

SELECT

    YEAR(order_date) AS order_year,

    product_category,

    ROUND(SUM(profit), 2) AS total_profit

FROM ecommerce_dev.gold.sales_master

GROUP BY

    YEAR(order_date),

    product_category

ORDER BY

    order_year,

    product_category;




-- ==========================================================
-- Profit By Customer
-- ==========================================================

SELECT

    customer_id,

    customer_name,

    ROUND(SUM(profit), 2) AS total_profit

FROM ecommerce_dev.gold.sales_master

GROUP BY

    customer_id,

    customer_name

ORDER BY

    total_profit DESC;





-- ==========================================================
-- Profit By Customer + Year
-- ==========================================================

SELECT

    customer_id,

    customer_name,

    YEAR(order_date) AS order_year,

    ROUND(SUM(profit), 2) AS total_profit

FROM ecommerce_dev.gold.sales_master

GROUP BY

    customer_id,

    customer_name,

    YEAR(order_date)

ORDER BY

    customer_name,

    order_year;
