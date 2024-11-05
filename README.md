
# STEDI Human Balance Analytics

This project leverages AWS Glue and other AWS services to process, transform, and analyze human balance data collected from various IoT devices. The project uses an ETL pipeline to clean, integrate, and prepare data for machine learning purposes.

## Project Overview

This project simulates a data pipeline using AWS Glue and S3 for a hypothetical health tech company, STEDI, focused on human balance analytics. The ETL pipeline reads customer and accelerometer data, sanitizes it, and organizes it into a series of zones (Landing, Trusted, and Curated) for analysis and machine learning model preparation.

## Table of Contents
1. [Project Structure](#project-structure)
2. [Data Workflow](#data-workflow)
3. [Technologies Used](#technologies-used)
4. [Setup Instructions](#setup-instructions)
5. [Scripts and Queries](#scripts-and-queries)
6. [Results and Queries](#results-and-queries)
7. [Contributing](#contributing)

## Project Structure

```
.
├── data/
│   ├── customer_landing/            # Raw customer data
│   ├── step_trainer_landing/         # Raw step trainer IoT data
│   ├── accelerometer_landing/        # Raw accelerometer data
│   └── curated/                      # Final processed data
├── scripts/
│   ├── customer_landing.sql          # SQL script for customer landing data
│   ├── accelerometer_landing.sql     # SQL script for accelerometer landing data
│   ├── customer_trusted_to_curated.py # Glue job script for transforming customer data
│   └── README.md                     # Project documentation
└── screenshots/
    ├── customer_landing.png          # Screenshot of customer_landing query result
    ├── accelerometer_landing.png     # Screenshot of accelerometer_landing query result
    ├── customer_trusted.png          # Screenshot of customer_trusted query result
```

## Data Workflow

### Objective

1. **Sanitize customer data**: Load customer records and filter those who consent to share data for research.
2. **Sanitize accelerometer data**: Load and filter accelerometer data of consenting customers.
3. **Integrate data**: Join customer and accelerometer datasets to create a curated dataset.
4. **Final Aggregation**: Join curated customer data with step trainer data for ML model preparation.

### Steps
1. **Landing Zone**: Raw data for `customer`, `step trainer`, and `accelerometer` data stored in respective directories.
2. **Trusted Zone**: Filtered data stored in `customer_trusted` and `accelerometer_trusted` Glue tables.
3. **Curated Zone**: Joined data stored in `customer_curated` Glue table for ML processing.

## Technologies Used

- **AWS Glue**: For ETL job orchestration and schema management.
- **AWS S3**: Data storage for different stages (Landing, Trusted, Curated zones).
- **Amazon Athena**: Querying and analyzing data tables.
- **Git**: Version control for scripts and documentation.

## Setup Instructions

### Prerequisites

1. **AWS Account**: Access to AWS Glue, S3, and Athena.
2. **Git**: Install Git for version control.

### Steps

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/smitpatel24680/stedi-human-balance-analytics.git
   cd stedi-human-balance-analytics
   ```

2. **Configure AWS Glue Jobs**:
   - Set up AWS Glue jobs based on the `customer_trusted_to_curated.py` script.
   - Adjust S3 paths as necessary to match your environment.

3. **Execute SQL Queries in Athena**:
   - Run `customer_landing.sql` and `accelerometer_landing.sql` queries on Athena.
   - Save the resulting Glue tables for use in ETL jobs.

4. **View Results**:
   - Check `screenshots/` for expected output examples from each table.

## Scripts and Queries

### SQL Scripts

- **customer_landing.sql**: Creates and queries the customer landing data table.
- **accelerometer_landing.sql**: Creates and queries the accelerometer landing data table.

### Python Scripts

- **customer_trusted_to_curated.py**: AWS Glue script to filter and curate customer data based on research consent.

## Results and Queries

Each landing and trusted dataset is queried in Athena, with example outputs saved as screenshots:
- **customer_landing.png**: Screenshot of raw customer data.
- **accelerometer_landing.png**: Screenshot of raw accelerometer data.
- **customer_trusted.png**: Filtered customer data based on consent.

## Contributing

Please open an issue or submit a pull request for any changes. All contributions are welcome to enhance and extend the functionality of this project.

