
-- CREATING A BASE DATABASE
--We first created a complete_joint_dataset which joins multiple tables together after analysing the relationships between 
--each table to confirm if there was a one-to-many, many-to-one or a many-to-many relationship for each of the join columns.
--We also included the rental_date column to help us split ties for rankings which had the same count of rentals at a 
--customer level - this helps us prioritise film categories which were more recently viewed.

DROP TABLE IF EXISTS complete_joint_dataset;
CREATE TEMP TABLE complete_joint_dataset AS
SELECT
  rental.customer_id,
  inventory.film_id,
  film.title,
  category.name AS category_name,
  -- also included rental_date for sorting purposes
  rental.rental_date
FROM dvd_rentals.rental
INNER JOIN dvd_rentals.inventory
  ON rental.inventory_id = inventory.inventory_id
INNER JOIN dvd_rentals.film
  ON inventory.film_id = film.film_id
INNER JOIN dvd_rentals.film_category
  ON film.film_id = film_category.film_id
INNER JOIN dvd_rentals.category
  ON film_category.category_id = category.category_id;
  
  
SELECT * FROM complete_joint_dataset limit 10;
  

-- CREATING A TABLE TO CALCULATE CATEGORY COUNT
--We then created a follow-up table which uses the complete_joint_dataset to aggregate our data 
--and generate a rental_count and the latest rental_date for our ranking purposes downstream.

DROP TABLE IF EXISTS category_counts;
CREATE TEMP TABLE category_counts AS
SELECT
  customer_id,
  category_name,
  COUNT(*) AS rental_count,
  MAX(rental_date) AS latest_rental_date
FROM complete_joint_dataset
GROUP BY
  customer_id,
  category_name;
  
  
SELECT * FROM category_counts limit 10;


-- CREATING TOTAL COUNTS TABLE
--We will then use this category_counts table to generate our total_counts table.

DROP TABLE IF EXISTS total_counts;
CREATE TEMP TABLE total_counts AS
SELECT
  customer_id,
  SUM(rental_count) AS total_count
FROM category_counts
GROUP BY
  customer_id;
  
SELECT * FROM total_counts limit 10;

--CREATING A TABLE FOR TOP CATEGORIES
--We can also use a simple DENSE_RANK window function to generate a ranking of categories for each customer.
--We will also split arbitrary ties by preferencing the category which had the most recent latest_rental_date 
--value we generated in the category_counts for this exact purpose. To further prevent any ties - we will also
--sort the category_name in alphabetical (ascending) order just in case!

DROP TABLE IF EXISTS top_categories;
CREATE TEMP TABLE top_categories AS
WITH ranked_cte AS (
  SELECT
    customer_id,
    category_name,
    rental_count,
    DENSE_RANK() OVER (
      PARTITION BY customer_id
      ORDER BY
        rental_count DESC,
        latest_rental_date DESC,
        category_name
    ) AS category_rank
  FROM category_counts
)
SELECT * FROM ranked_cte
WHERE category_rank <= 2;

SELECT * FROM top_categories limit 10;

--CREATING THE AVERAGE CATEGORY COUNT TABLE
--Next we will need to use the category_counts table to generate the average aggregated rental count for each category 
--rounded down to the nearest integer using the FLOOR function


DROP TABLE IF EXISTS average_category_count;
CREATE TEMP TABLE average_category_count AS
SELECT
  category_name,
  FLOOR(AVG(rental_count)) AS category_average
FROM category_counts
GROUP BY category_name;

SELECT * FROM average_category_count


--CREATING TOP CATEGORY PERCENTILE TABLE
--Now we need to compare each customer’s top category rental_count to all other DVD Rental Co customers - we do this using 
--a combination of a LEFT JOIN and a PERCENT_RANK window function ordered by descending rental count to show the required top N% customer insight.
--We will also use a CASE WHEN to replace a 0 ranking value to 1 as it doesn’t make sense for the customer to be in the Top 0%!


DROP TABLE IF EXISTS top_category_percentile;
CREATE TEMP TABLE top_category_percentile AS
WITH calculated_cte AS (
SELECT
  top_categories.customer_id,
  top_categories.category_name AS top_category_name,
  top_categories.rental_count,
  category_counts.category_name,
  top_categories.category_rank,
  PERCENT_RANK() OVER (
    PARTITION BY category_counts.category_name
    ORDER BY category_counts.rental_count DESC
  ) AS raw_percentile_value
FROM category_counts
LEFT JOIN top_categories
  ON category_counts.customer_id = top_categories.customer_id
)
SELECT
  customer_id,
  category_name,
  rental_count,
  category_rank,
  CASE
    WHEN ROUND(100 * raw_percentile_value) = 0 THEN 1
    ELSE ROUND(100 * raw_percentile_value)
  END AS percentile
FROM calculated_cte
WHERE
  category_rank = 1
  AND top_category_name = category_name;
  
  
 SELECT * FROM top_category_percentile LIMIT 10;
 
--CREATING FIRST CATEGORY INSIGHTS
--Let’s now compile all of our previous temporary tables into a single category_insights table with what 
--we have so far - we will use our most recently generated top_category_percentile table as the base and 
--LEFT JOIN our average table to generate an average_comparison column.


DROP TABLE IF EXISTS first_category_insights;
CREATE TEMP TABLE first_category_insights AS
SELECT
  base.customer_id,
  base.category_name,
  base.rental_count,
  base.rental_count - average.category_average AS average_comparison,
  base.percentile
FROM top_category_percentile AS base
LEFT JOIN average_category_count AS average
  ON base.category_name = average.category_name;
  
  
SELECT * FROM first_category_insights LIMIT 10;

--CREATING SECOND CATEGORY INSIGHTS
--Our second ranked category insight is pretty simple as we only need our top_categories table and the total_counts 
--table to process our insights.The only thing to note here is that we’ll need to cast one of our fraction components 
--of our total_percentage column to avoid the dreaded integer floor division!


DROP TABLE IF EXISTS second_category_insights;
CREATE TEMP TABLE second_category_insights AS
SELECT
  top_categories.customer_id,
  top_categories.category_name,
  top_categories.rental_count,
  -- need to cast as NUMERIC to avoid INTEGER floor division!
  ROUND(
    100 * top_categories.rental_count::NUMERIC / total_counts.total_count
  ) AS total_percentage
FROM top_categories
LEFT JOIN total_counts
  ON top_categories.customer_id = total_counts.customer_id
WHERE category_rank = 2;


SELECT * FROM second_category_insights LIMIT 10;



  
  
 
 
