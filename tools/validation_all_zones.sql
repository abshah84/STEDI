/* ============================================================
   LANDING ZONE VALIDATION
   ============================================================ */

/* Count of customer_landing table */
SELECT COUNT(*) AS customer_landing_count
FROM customer_landing;

/* Count of accelerometer_landing table */
SELECT COUNT(*) AS accelerometer_landing_count
FROM accelerometer_landing;

/* Count of step_trainer_landing table */
SELECT COUNT(*) AS step_trainer_landing_count
FROM step_trainer_landing;


/* ============================================================
   TRUSTED ZONE VALIDATION
   ============================================================ */

/* Count of customer_trusted table */
SELECT COUNT(*) AS customer_trusted_count
FROM customer_trusted;

/* Count of accelerometer_trusted table */
SELECT COUNT(*) AS accelerometer_trusted_count
FROM accelerometer_trusted;

/* Count of step_trainer_trusted table */
SELECT COUNT(*) AS step_trainer_trusted_count
FROM step_trainer_trusted;


/* ============================================================
   CURATED ZONE VALIDATION
   ============================================================ */

/* Count of customers_curated table */
SELECT COUNT(*) AS customer_curated_count
FROM customer_curated;

/* Count of machine_learning_curated table */
SELECT COUNT(*) AS machine_learning_curated_count
FROM machine_learning_curated;
