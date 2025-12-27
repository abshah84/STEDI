# STEDI Human Balance Analytics – Data Lakehouse Project  
### Udacity DEND – Spark and Human Balance

This project builds a complete AWS data lakehouse for the STEDI Step Trainer team. Using AWS Glue, Spark, S3, and Athena, we ingest raw JSON data, sanitize it, curate it, and produce a final dataset used to train a machine learning model that detects human steps.

The pipeline follows the required **Landing → Trusted → Curated → Machine Learning** architecture, with validation checks at each stage.

## 📁 Project Structure

STEDI/
│
├── data/
│   ├── customer/
│   │   └── landing/
│   ├── accelerometer/
│   │   └── landing/
│   ├── step_trainer/
│       └── landing/
│
├── glue_jobs/
│   ├── customer_landing_to_trusted.py
│   ├── accelerometer_landing_to_trusted.py
│   ├── step_trainer_landing_to_trusted.py
│   ├── customer_trusted_to_curated.py
│   ├── machine_learning_curated.py
│
├── sql/
│   ├── customer_landing.sql
│   ├── accelerometer_landing.sql
│   ├── step_trainer_landing.sql
│
├── tools/
│   ├── validation_all_zones.sql
│
├── screenshots/
│   ├── 01_customer_landing_count.png
│   ├── 02_accelerometer_landing_count.png
│   ├── 03_step_trainer_landing_count.png
│   ├── 04_customer_trusted_count.png
│   ├── 05_accelerometer_trusted_count.png
│   ├── 06_step_trainer_trusted_count.png
│   ├── 07_customer_curated_count.png
│   ├── 08_machine_learning_curated_count.png
│
└── README.md


---

##  Project Workflow

### 1. Landing Zone  
Raw JSON files were extracted from the Udacity GitHub repo and stored locally under:

data/customer/landing/
data/accelerometer/landing/
data/step_trainer/landing/


These files were then uploaded to S3 under corresponding prefixes. Glue tables were created manually using the SQL DDLs in the 'sql/' folder.

**Row Count Validation:**
| Table                  | Expected | Actual |
|------------------------|----------|--------|
| customer_landing       | 956      | ✔      |
| accelerometer_landing  | 81273    | ✔      |
| step_trainer_landing   | 28680    | ✔      |

Screenshots are included in the 'screenshots/' folder.

---

### 2. Trusted Zone  
Glue jobs filter and sanitize the landing data to include only customers who consented to share data.

**Jobs:**
- 'customer_landing_to_trusted.py' 
  Filters customers with a non-null 'shareWithResearchAsOfDate'.

- 'accelerometer_landing_to_trusted.py'  
  Joins accelerometer data with consenting customers.

- 'step_trainer_landing_to_trusted.py'  
  Filters step trainer records for valid serial numbers linked to consenting customers.

**Row Count Validation:**
| Table                  | Expected | Actual |
|------------------------|----------|--------|
| customer_trusted       | 482      | ✔      |
| accelerometer_trusted  | 40981    | ✔      |
| step_trainer_trusted   | 14460    | ✔      |

---

### 3. Curated Zone  
This stage identifies customers who both consented and have accelerometer activity.

**Job:**
- 'customer_trusted_to_curated.py' 
  Produces the curated customer list.

**Row Count Validation:**
| Table             | Expected | Actual |
|-------------------|----------|--------|
| customer_curated  | 482      | ✔      |

---

### 4. Machine Learning Curated Zone  
This final dataset joins step trainer readings with accelerometer data based on matching timestamps.

**Job:**
- 'machine_learning_curated.py'  
  Joins 'step_trainer_trusted.sensorReadingTime' with 'accelerometer_trusted.timeStamp'.

**Row Count Validation:**
| Table                    | Expected | Actual |
|--------------------------|----------|--------|
| machine_learning_curated | 43681    | ✔      |

---

## Validation  
All SQL queries used to validate row counts are stored in:

tools/validation_all_zones.sql


These were run in Athena to confirm that each transformation produced the expected results.

---

##  Technologies Used
- AWS Glue (Spark ETL)
- AWS S3 (Data Lake Storage)
- AWS Athena (SQL Query Engine)
- PySpark
- JSON (input format)
- Parquet (optional for optimized storage)

---

##  Notes & Best Practices
- All Glue jobs use Data Catalog sources to ensure schema consistency.
- SQL Query nodes were preferred over Join nodes in Glue Studio to avoid Spark join duplication issues.
- S3 folders were cleared before re-running jobs to prevent stale data.
- Athena tables were recreated or repaired after each ETL stage.

---

##  Project Complete  
This repository includes everything required for the Udacity STEDI project:

- Landing zone SQL DDLs  
- Glue job scripts  
- Validation SQL  
- Screenshots of Athena results  
- README documentation  

The pipeline is fully functional, validated, and aligned with the project rubric.
