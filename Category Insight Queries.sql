
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
  
 
 
