-----------------------------------------------------------------------------------------------------------------------------------
--for customer_df--
-----------------------------------------------------------------------------------------------------------------------------------
select * from customer_df

--is customer_unique_id similar to customer_id??--

select customer_id from customer_df where customer_id  in  
(select customer_unique_id from customer_df)  -- null means no theu are completly diffrent 

-- making pk , fk and changing data types--

ALTER TABLE customer_df
ADD CONSTRAINT customer_pkey 	PRIMARY KEY	(customer_id)--done


ALTER TABLE customer_df
ALTER COLUMN customer_zip_code_prefix TYPE INTEGER USING
customer_zip_code_prefix ::integer;--done

ALTER TABLE customer_df
ALTER COLUMN customer_id TYPE UUID USING
customer_id ::uuid;

--adding new column geolocation_id  for m,akeing further realtion with geolocation table--

ALTER TABLE customer_df ADD COLUMN geolocation_id INTEGER;--done

ALTER TABLE customer_df ADD CONSTRAINT geolocation_fk FOREIGN KEY (geolocation_id) REFERENCES geolocation_df(geolocation_id)--done





---------------------------------------------------------------------------------------------------------------------------------------------
--geoloction_df--
-----------------------------------------------------------------------------------------------------------------------------------
select * from geolocation_df

---adding pk and  fk and changing the data types--
-- since no one able to be a pk for the table , lts add a new column (unique) make it pk
ALTER TABLE geolocation_df ADD COLUMN geolocation_id SERIAL PRIMARY KEY--done

ALTER TABLE geolocation_df ALTER COLUMN geolocation_zip_code_prefix TYPE INTEGER USING geolocation_zip_code_prefix::integer;--done
ALTER TABLE geolocation_df ALTER COLUMN geolocation_lat TYPE NUMERIC;--done
ALTER TABLE geolocation_df ALTER COLUMN geolocation_lng TYPE NUMERIC;--done
ALTER TABLE geolocation_df ALTER COLUMN geolocation_city TYPE VARCHAR(255);--done
ALTER TABLE geolocation_df ALTER COLUMN geolocation_state	 TYPE VARCHAR(255);--done


---------------------------------------------------------------------------------------------------------------------------------------------
--order_items_df--
-----------------------------------------------------------------------------------------------------------------------------------
select * from order_items_df

ALTER TABLE order_items_df ALTER COLUMN order_id TYPE UUID USING order_id::uuid;--done
ALTER TABLE order_items_df ALTER COLUMN product_id TYPE UUID USING product_id::uuid;--done
ALTER TABLE order_items_df ALTER COLUMN seller_id TYPE UUID USING seller_id::uuid;--done

ALTER TABLE order_items_df ALTER COLUMN price TYPE NUMERIC USING price::numeric;--done
ALTER TABLE order_items_df ALTER COLUMN freight_value TYPE NUMERIC USING freight_value::numeric;--done
ALTER TABLE order_items_df ALTER COLUMN shipping_limit_date TYPE TIMESTAMP WITH TIME ZONE 
USING shipping_limit_date::timestamp with time zone--done


ALTER TABLE order_items_df ADD PRIMARY KEY (order_id, product_id,seller_id,order_item_id);

ALTER TABLE order_items_df ADD CONSTRAINT fk_order_items_orders FOREIGN KEY (order_id) REFERENCES order_orders_df(order_id);

ALTER TABLE order_items_df ALTER COLUMN order_item_id TYPE INTEGER USING order_item_id::INTEGER


SELECT order_id
FROM order_items_df
WHERE order_id NOT IN (SELECT order_id FROM order_orders_df);



---------------------------------------------------------------------------------------------------------------------------------------------
--order_orders_df--
---------------------------------------------------------------------------------------------------------------------------------------------
select * from order_orders_df


ALTER TABLE order_orders_df ALTER COLUMN order_id TYPE UUID USING order_id::uuid;--done
ALTER TABLE order_orders_df ALTER COLUMN customer_id TYPE UUID USING customer_id::uuid--done
ALTER TABLE order_orders_df ALTER COLUMN order_purchase_timestamp	TYPE TIMESTAMP WITH TIME ZONE 
USING order_purchase_timestamp	::timestamp with time zone--done

ALTER TABLE order_orders_df ALTER COLUMN order_delivered_carrier_date	TYPE TIMESTAMP WITH TIME ZONE 
USING order_delivered_carrier_date	::timestamp with time zone;--done

ALTER TABLE order_orders_df ALTER COLUMN order_delivered_customer_date	TYPE TIMESTAMP WITH TIME ZONE 
USING order_delivered_customer_date::timestamp with time zone;--done

ALTER TABLE order_orders_df ALTER COLUMN order_estimated_delivery_date		TYPE TIMESTAMP WITH TIME ZONE 
USING order_estimated_delivery_date::timestamp with time zone--done

ALTER TABLE order_orders_df ALTER COLUMN order_purchase_timestamp	TYPE TIMESTAMP WITH TIME ZONE 
USING order_purchase_timestamp	::timestamp with time zone--done

ALTER TABLE order_orders_df ALTER COLUMN order_status type varchar(255)--done


ALTER TABLE order_orders_df ADD PRIMARY KEY (order_id)

ALTER TABLE order_orders_df
ADD CONSTRAINT fk_customer_id
FOREIGN KEY (customer_id)
REFERENCES customer_df(customer_id);

---------------------------------------------------------------------------------------------------------------------------------------------
--products_df--
---------------------------------------------------------------------------------------------------------------------------------------------
select * from products_df

ALTER TABLE products_df ALTER COLUMN product_id TYPE UUID USING product_id::uuid;--done
ALTER TABLE products_df ALTER COLUMN product_name_lenght	TYPE NUMERIC 	USING product_name_lenght::numeric ; 
ALTER TABLE products_df ALTER COLUMN product_description_lenght	TYPE NUMERIC 	USING product_description_lenght::numeric
ALTER TABLE products_df ALTER COLUMN product_weight_g		TYPE NUMERIC 	USING product_weight_g	::numeric
ALTER TABLE products_df ALTER COLUMN product_length_cm		TYPE NUMERIC 	USING product_length_cm	::numeric
ALTER TABLE products_df ALTER COLUMN product_height_cm		TYPE NUMERIC 	USING product_height_cm	::numeric
ALTER TABLE products_df ALTER COLUMN product_width_cm	TYPE NUMERIC 	USING product_width_cm::numeric
ALTER TABLE products_df ALTER COLUMN product_photos_qty	TYPE NUMERIC USING product_photos_qty::numeric

ALTER TABLE products_df ADD CONSTRAINT pk_product_id PRIMARY KEY (product_id)

ALTER TABLE products_df
ADD CONSTRAINT product_category_name_fkey
FOREIGN KEY (product_category_name) REFERENCES product_category_df(product_category_name);


SELECT DISTINCT product_category_name
FROM products_df
WHERE product_category_name NOT IN (SELECT product_category_name FROM product_category_df);

---------------------------------------------------------------------------------------------------------------------------------------------

--order_payments_df--

---------------------------------------------------------------------------------------------------------------------------------------------
select * from order_payments_df

ALTER TABLE order_payments_df
ALTER COLUMN payment_sequential TYPE INTEGER USING payment_sequential::INTEGER,
ALTER COLUMN payment_installments TYPE INTEGER USING payment_installments::INTEGER,
ALTER COLUMN payment_value TYPE NUMERIC USING payment_value::NUMERIC;

ALTER TABLE  order_payments_df
ALTER COLUMN order_id TYPE UUID USING order_id::uuid

ALTER TABLE  order_payments_df
ALTER COLUMN payment_type TYPE VARCHAR(255)

ALTER TABLE order_payments_df
ADD CONSTRAINT order_payments_pkey PRIMARY KEY (order_id, payment_sequential);

--ALTER TABLE order_payments_df
--ADD CONSTRAINT fk_order_payments_orders
--FOREIGN KEY (order_id) REFERENCES order_orders_df(order_id);


---------------------------------------------------------------------------------------------------------------------------------------------
--order_reviews_df--

---------------------------------------------------------------------------------------------------------------------------------------------

select * from order_reviews_df

ALTER  TABLE  order_reviews_df
ALTER COLUMN review_id TYPE UUID USING review_id::uuid,
ALTER COLUMN order_id TYPE UUID USING order_id::uuid

ALTER TABLE order_reviews_df
ALTER COLUMN review_comment_title TYPE VARCHAR(255),
ALTER COLUMN  review_comment_message TYPE  VARCHAR(255



select review_id , count(*) from order_reviews_df group by review_id having count(*)>1
-- sql for finding dumplicate review_id and number of order_id its realted to --
SELECT review_id, COUNT(DISTINCT order_id)
FROM order_reviews_df
GROUP BY review_id
HAVING COUNT(DISTINCT order_id) > 1;

-- lets assign this duplicate review_id a new uuid (random)
update order_reviews_df
set review_id=gen_random_uuid()
where review_id in (SELECT review_id FROM order_reviews_df GROUP BY review_id HAVING count(*) > 1)
-- lets amke review_id key here --
ALTER TABLE order_reviews_df ADD CONSTRAINT order_reviews_pkey PRIMARY KEY (review_id);

--ALTER TABLE order_reviews_df
--ADD CONSTRAINT order_reviews_order_id_fkey
--FOREIGN KEY (order_id) REFERENCES order_orders_df(order_id);

SELECT *
FROM order_reviews_df
WHERE order_id NOT IN (SELECT order_id FROM order_orders_df);

--ALTER TABLE order_reviews_df
--ADD CONSTRAINT fk_order_reviews_orders
--FOREIGN KEY (order_id) REFERENCES order_orders_df(order_id);
---------------------------------------------------------------------------------------------------------------------------------------------
--product_category_df--

---------------------------------------------------------------------------------------------------------------------------------------------

select * from product_category_df

ALTER TABLE product_category_df
ALTER COLUMN product_category_name TYPE VARCHAR(255),
ALTER COLUMN  product_category_name_english TYPE  VARCHAR(255)

ALTER TABLE product_category_df ADD CONSTRAINT product_category_name_pk PRIMARY KEY (product_category_name);

INSERT INTO product_category_df (product_category_name,product_category_name_english)  -- Include other relevant columns
VALUES ('pc_gamer', 'pc_gamer'); -- Provide values for other columns as well

INSERT INTO product_category_df (product_category_name,product_category_name_english)  -- Include other relevant columns
VALUES ('portateis_cozinha_e_preparadores_de_alimentos', 'portateis_kitchen_e_food_preparators'); -- Provide values for other columns as well
---------------------------------------------------------------------------------------------------------------------------------------------

--sellers_df--

---------------------------------------------------------------------------------------------------------------------------------------------
select * from sellers_df

ALTER TABLE sellers_df
ALTER COLUMN seller_city TYPE VARCHAR(255),
ALTER COLUMN  seller_state TYPE  VARCHAR(255),
ALTER COLUMN seller_zip_code_prefix TYPE INTEGER USING seller_zip_code_prefix::integer,
ALTER COLUMN seller_id TYPE UUID USING seller_id::uuid

ALTER TABLE sellers_df
ADD COLUMN geolocation_id INTEGER;


UPDATE sellers_df
SET geolocation_id = (SELECT geolocation_id
                            FROM geolocation_df
                            WHERE geolocation_zip_code_prefix = sellers_df.seller_zip_code_prefix
                              AND geolocation_city = sellers_df.seller_city and geolocation_state = sellers_df.seller_state
                            LIMIT 1);
ALTER TABLE sellers_df
ADD CONSTRAINT sellers_pkey PRIMARY KEY (seller_id);
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
"1. Analyze sales and revenue trends over time (daily, weekly, monthly) to identify patterns, growth rates, and potential seasonality"
--
--
 
select * from order_orders_df

select * from order_items_df


------
--Monthly Totals--
with orderRevenue as(
select  oi.order_id ,
		oi.product_id,
		count(*) as quantity,
		sum(oi.price) as revenue
from order_items_df as oi 
group by 
 oi.order_id,
 oi.product_id

), 
mergeOrder as(

select oo.* ,
		orv.quantity,
		orv.revenue
from 
order_orders_df  as oo
left join
orderRevenue as orv
on oo.order_id=orv.order_id
)
select 
       
		to_char(date_trunc('month',mo.order_purchase_timestamp),'YYYY-MM-DD') as month,
		sum(mo.revenue) as total_revenue,
		sum(quantity) as total_quantity
		
from 
mergeOrder as mo
group by month
order by month;


--Daily Totals--
with orderRevenue as (
	select 
		order_id,
		product_id,
		sum(price)as revenue,
		count(*) as quantity 
	from order_items_df 
	group by 
	product_id,
	order_id
      ),
daily_order as (
       select oo.*,
	   orv.revenue,
	   orv.quantity
	   from 
	   order_orders_df as oo
	   left join 
	   orderRevenue as orv
	   on
	   oo.order_id=orv.order_id
	)
	select 
			to_char(date_trunc('day',dor.order_purchase_timestamp),'YYYY-MM-DD') as day,
			sum(dor.revenue) as daily_revenue,
			sum(dor.quantity)as daily_quantity
	 from daily_order as dor
	 group by day
	order by day
--weakly Totals--

with order_revenue as (
   select 
     	product_id,
		order_id,
		sum(price) as revenue,
		count(*) as quantity
		from  order_items_df
		group by product_id,order_id
	), 
	 weakly_order as(
    select 
	oo.*,
	dr.revenue,
	dr.quantity
	from 
	order_orders_df as oo 
	left join
	order_revenue as dr
	on oo.order_id=dr.order_id
	)
    select 
	   to_char(date_trunc('week',wo.order_purchase_timestamp),'YYYY-MM-DD') as weak,
	   sum(wo.revenue) as weakly_revenue,
	   sum(wo.quantity) as weakly_quantity

	   from weakly_order as wo
	   group by weak
	   order by weak

--- for tableau specific --------------------------------------------------------------------------------------------
WITH Order_revenue AS (
    SELECT
        order_id,
        product_id,  -- Include product_id 
        SUM(price) AS revenue,
        COUNT(*) AS quantity 
    FROM order_items_df
    GROUP BY order_id, product_id
), daily_sales AS (
    SELECT
        DATE(oo.order_purchase_timestamp) AS sales_day,
        orv.product_id,  -- Include product_id 
        SUM(revenue) AS daily_revenue,
        SUM(quantity) AS daily_quantity
    FROM order_orders_df AS oo
    JOIN Order_revenue AS orv ON oo.order_id = orv.order_id
    GROUP BY sales_day, orv.product_id 
), weekly_sales AS (
SELECT
        DATE(date_trunc('week', order_purchase_timestamp)) AS sales_week,
        orv.product_id,  -- Include product_id 
        SUM(revenue) AS weekly_revenue,
        SUM(quantity) AS weekly_quantity
    FROM order_orders_df AS oo
    JOIN Order_revenue AS orv ON oo.order_id = orv.order_id
    GROUP BY sales_week, orv.product_id
), monthly_sales AS (
       SELECT
        DATE(date_trunc('month', order_purchase_timestamp)) AS sales_month,
         orv.product_id,  -- Include product_id 
        SUM(revenue) AS monthly_revenue,
        SUM(quantity) AS monthly_quantity
    FROM order_orders_df AS oo
    JOIN Order_revenue AS orv ON oo.order_id = orv.order_id
    GROUP BY sales_month, orv.product_id
)


SELECT
    ds.sales_day,
        ds.product_id,
    ds.daily_revenue,
    ds.daily_quantity,
    ws.sales_week,
    ws.weekly_revenue,
    ws.weekly_quantity,
    ms.sales_month,
    ms.monthly_revenue,
    ms.monthly_quantity


FROM daily_sales AS ds
JOIN weekly_sales AS ws ON ds.sales_day BETWEEN ws.sales_week AND ws.sales_week + interval '6 days' and ds.product_id = ws.product_id 
JOIN monthly_sales AS ms ON ds.sales_day BETWEEN ms.sales_month AND ms.sales_month + interval '1 month - 1 day'and ds.product_id = ms.product_id  

	   ---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------

"2. Top-Selling Products: Identify the best-selling products by revenue or quantity. Uses products_df and order_items_df."
--
-- for revenue---------------------------------------------------------
 select * from products_df
 select * from order_items_df


select  pd.product_id,
		pd.product_category_name,
		sum(oi.price) as revenue,
		sum(oi.order_item_id) as quantity
	
from
	products_df as pd
join 
	order_items_df as oi
on 
	pd.product_id=oi.product_id
group by 
	pd.product_id,pd.product_category_name
order by
	revenue desc
limit 10

--quantity-------------------------------------------------------------------------------


select  pd.product_id,
		pd.product_category_name,
		sum(oi.price) as revenue,
		sum(oi.order_item_id) as quantity
	
from
	products_df as pd
join 
	order_items_df as oi
on 
	pd.product_id=oi.product_id
group by 
	pd.product_id,pd.product_category_name
order by
	quantity desc
limit 10




SELECT
    pd.product_id,
    pd.product_category_name,
    
        DATE(oi.shipping_limit_date) as date,
    SUM(oi.price) AS total_revenue,
    SUM(oi.order_item_id) AS total_quantity,
    RANK() OVER (ORDER BY SUM(oi.price) DESC) as revenue_rank,
    RANK() OVER (ORDER BY SUM(oi.order_item_id) DESC) as quantity_rank
FROM
    products_df AS pd
JOIN
    order_items_df AS oi ON pd.product_id = oi.product_id
GROUP BY
    pd.product_id, pd.product_category_name,  DATE(oi.shipping_limit_date)
ORDER BY revenue_rank, quantity_rank
LIMIT 10;

 ---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------

"3. Sales by Location (Customer Location): Analyze sales and revenue based on customer location (city and state). Uses customer_df and geolocation_df. 
(You also have seller location, but focusing on customer location is often more insightful for sales analysis)"
--
--
select * from customer_df

select * from geolocation_df

select * from order_items_df

--select * from seller_df

select * from order_orders_df

WITH a AS (
    SELECT
        oi.order_id,
        oi.price,
        oo.customer_id,
		oi.order_item_id
    FROM order_items_df AS oi
    JOIN order_orders_df AS oo ON oi.order_id = oo.order_id
),
b AS (
    SELECT
        a.order_id,
        a.price,
		a.order_item_id as order_item_quantity,
        cd.customer_id, 
        cd.customer_city,
        cd.customer_state,
        cd.customer_zip_code_prefix
    FROM a
    JOIN customer_df AS cd ON a.customer_id = cd.customer_id
)
SELECT b.order_id,
    b.customer_city,
    b.customer_state,
    b.customer_id, 
    b.customer_zip_code_prefix,
    SUM(b.price) AS total_revenue,
	SUM(b.order_item_quantity) AS total_quantity_sold
FROM b
join geolocation_df as g on  b.customer_zip_code_prefix=g.geolocation_zip_code_prefix
GROUP BY
    b.customer_city, b.customer_state, b.customer_id, b.customer_zip_code_prefix,b.order_id
ORDER BY
    total_revenue DESC;




--
SELECT
    g.geolocation_zip_code_prefix,
    SUM(oi.price) AS total_revenue,
    SUM(oi.order_item_id) AS total_quantity_sold,  
    g.geolocation_lat AS latitude,
    g.geolocation_lng AS longitude
FROM order_items_df AS oi
JOIN order_orders_df AS oo ON oi.order_id = oo.order_id
JOIN customer_df AS cd ON oo.customer_id = cd.customer_id
JOIN geolocation_df AS g ON cd.customer_zip_code_prefix = g.geolocation_zip_code_prefix
GROUP BY g.geolocation_zip_code_prefix, g.geolocation_lat, g.geolocation_lng  -- Group by zip code and coordinates
ORDER BY total_revenue DESC;
--



---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
--4. Average Order Value: Calculate the average order value overall and potentially by customer location. Uses order_orders_df and order_items_df.
--
--

select * from order_orders_df
select * from order_items_df

--Overall avg order value
with order_value_cte as(
select oi.order_id,sum(oi.price) as order_value 
from 
	order_orders_df as oo 
join 
	order_items_df as oi 
on
	oo.order_id=oi.order_id

group by oi.order_id
)
select avg(order_value) as avergae_order_value from order_value_cte





--Average order value By Customer  Location

with order_value_cte as(
select oo.order_id,sum(oi.price) as order_value ,oo.customer_id
from 
	order_orders_df as oo 
join 
	order_items_df as oi 
on
	oo.order_id=oi.order_id

group by oo.order_id,oo.customer_id

) 
select avg(ov.order_value) as avg_order_value,cd.customer_zip_code_prefix,cd.customer_city,cd.customer_state from 

order_value_cte as ov
join customer_df as cd
on ov.customer_id=cd.customer_id
group by cd.customer_city ,cd.customer_state,cd.customer_zip_code_prefix

---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
--	6. Review Analysis (Basic): Determine the average customer rating overall and by product or category. Uses order_reviews_df, products_df, and order_items_df.
--
--
select * from order_reviews_df
select * from products_df
select * from order_items_df

--average ratings overall
select avg(review_score)
from order_reviews_df 

--avg rating by product or category 

select pd.product_category_name,
		avg(orv.review_score) as average_ratings
from 
order_reviews_df as orv 
join order_items_df as oi on orv.order_id=oi.order_id
join products_df as pd on pd.product_id=oi.product_id
group by pd.product_category_name
order by average_ratings desc

---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
--Shipping Time Analysis: Calculate average shipping time and identify potential delays. 
--This uses order_orders_df (order and delivery timestamps) and possibly order_items_df (if we want to analyze shipping time by product).
--
--
select * from order_orders_df
select * from order_items_df


select 
	avg(shipping_time_delay) as avg_time_in_days
from(
	select oo.order_id,	
			oo.customer_id,
			Extract(day  from (oo.order_delivered_customer_date-oo.order_purchase_timestamp)) as shipping_time_delay
	from  
		order_orders_df as oo
	

) as shipping_delays 

------------------------------------
WITH ShippingTimes AS (
    SELECT
        oo.order_id,
        EXTRACT(DAY FROM (oo.order_delivered_customer_date - oo.order_purchase_timestamp)) AS shipping_time_days,
        EXTRACT(DAY FROM (oo.order_estimated_delivery_date-oo.order_delivered_customer_date)) AS shipping_delay_days
    FROM order_orders_df AS oo
    WHERE oo.order_delivered_customer_date IS NOT NULL  
)
SELECT
    shipping_time_days,
    shipping_delay_days
FROM ShippingTimes;





---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------

--8. Seller Performance (Sales and Ratings): Analyze which sellers have the highest sales and best customer ratings. Uses sellers_df, order_items_df, and order_reviews_df.
--
--

select * from sellers_df
select * from order_items_df
select * from order_reviews_df

--sellers with highest sale
with sales as(
select oi.seller_id,sum(oi.price) as total_sales
from order_items_df as oi
group by oi.seller_id
),
 ratings as (
select oi.seller_id ,avg(orv.review_score ) as avg_rating from 
order_items_df as oi 
join 
order_reviews_df as orv
on oi.order_id=orv.order_id
group by oi.seller_id
)
select sd.seller_id,
		sd.seller_zip_code_prefix,
		sd.seller_state,
		sd.seller_city,
		coalesce(s.total_sales,0) as total_sales,
		coalesce(rt.avg_rating,0) as avg_ratings
from sellers_df as sd 
left join sales as s on sd.seller_id=s.seller_id
left join ratings as rt on sd.seller_id=rt.seller_id
order by total_sales desc,
		avg_rating desc
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------

--END-----------------------------------------------------------------------------------------------------------------------------------------------------------------------