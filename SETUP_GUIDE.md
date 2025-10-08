# Project Setup Guide

This guide provides step-by-step instructions to set up and run the E-commerce Data Pipeline project locally using Docker and Docker Compose.

## Step 1: Clone the Repository

```bash
git clone https://github.com/brankowss/scala-spark-ecommerce-pipeline.git
cd scala-spark-ecommerce-pipeline
```

## Step 2: Prepare and Configure the Project

This is a crucial step that prepares all necessary directories, dependencies, and configurations before you build the Docker images.

### 2.1. Create Directories and Set Permissions
These folders are required by the pipeline for data generation and reporting, and they need to be writable by the Docker containers.

```bash
mkdir -p generated_data reports jars db_init
sudo chmod -R 777 generated_data reports 
```

### 2.2. Download the PostgreSQL JDBC Driver
The Spark job needs this driver to connect to the PostgreSQL data warehouse.
```bash
wget -P ./jars wget -P ./jars https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
```

### 2.3. Configure Database Credentials

This step sets up the username and password for your PostgreSQL database.

1.  **Create the `.env` file** by copying the example file. Run this command in your terminal:
    ```bash
    cp .env.example .env
    ```

2.  **Open the new `.env` file** in your code editor (e.g., VS Code). You will see the following content:
    ```env
    # 1. PostgreSQL Data Warehouse Credentials
    POSTGRES_USER=your_usersname
    POSTGRES_PASSWORD=your_secure_password
    POSTGRES_DB=ecommerce_dwh

    # 2. Metabase Application Database Credentials
    MB_DB_TYPE=postgres
    MB_DB_DBNAME=metabase_app_db
    MB_DB_PORT=5432
    MB_DB_USER= # Should match POSTGRES_USER
    MB_DB_PASS= # Should match POSTGRES_PASSWORD
    MB_DB_HOST=postgres-warehouse

    # 3. Twitter API Credentials (for Bonus Task)
    # This is required only for the bonus streaming pipeline (EP-12).
    TWITTER_BEARER_TOKEN=your_twitter_bearer_token_here

    # 4. These are used by Jenkins to send pipeline status and alert emails.
    NOTIFICATION_EMAIL=your_email@example.com
    JENKINS_SENDER_EMAIL=your_sender_gmail@gmail.com
    JENKINS_EMAIL_APP_PASSWORD=your_16_character_app_password
    ```
    ## Note (Email): To get a JENKINS_EMAIL_APP_PASSWORD, you must enable 2-Factor Authentication on your Google Account and generate a 16-character "App Password" for Jenkins.

    ## Note (Twitter): To get a TWITTER_BEARER_TOKEN, you must have a Twitter Developer account and create an App within a Project on the Developer Portal.

3.  **Save the file** after making your changes. 

## Step 3: Build and Start All Services

This command will build the custom Docker images and start all services in the background. This may take several minutes the first time.

```bash
docker compose up -d --build
```

## Step 4: Initialize Infrastructure

After the containers are running, perform these one-time initialization steps.

1.  **Initialize Hive Metastore Schema:**
    This prepares the PostgreSQL database to be used by Hive.
    ```bash
    docker compose exec hive-metastore /opt/hive/bin/schematool -dbType postgres -initSchema
    ```
    After it completes, restart the Hive services:
    ```bash
    docker compose restart hive-metastore hive-server
    ```

2.  **Take HDFS out of Safe Mode:**
    Allow the pipeline to write files to the Data Lake.
    ```bash
    docker compose exec namenode hdfs dfsadmin -safemode leave
    ```

## Step 5: Configure Jenkins & Automate Pipelines

This section covers the complete setup of Jenkins, from initial configuration to creating and scheduling the two separate pipelines required by the project.

### 5.1. Access Jenkins & Initial Setup

1.  **Access Jenkins:** Open `http://localhost:8080` in your browser.
2.  **Unlock Jenkins:** The first time you access it, Jenkins will ask for an administrator password. You can get this password by running the following command in your terminal:
    ```bash
    docker compose exec jenkins cat /var/jenkins_home/secrets/initialAdminPassword
    ```
    Copy the password from your terminal and paste it into the Jenkins UI.
3.  **Install Plugins:** Choose **Install suggested plugins**.
4.  **Create Admin User:** Create your first admin user account.

### 5.2. Configure Jenkins System Email
Now, we'll configure Jenkins to use your Gmail account to send emails.

1.  Navigate to **Manage Jenkins > System**.
2.  Scroll down to the **E-mail Notification** section at the bottom.
3.  Fill in the following fields:
    -   **SMTP server**: `smtp.gmail.com`
    -   Click **Advanced...**.
    -   Check **Use SMTP Authentication**.
    -   **User Name**: Enter your full Gmail address (the same one as in `JENKINS_SENDER_EMAIL`).
    -   **Password**: Enter your **16-character Google App Password** (the same one as in `JENKINS_EMAIL_APP_PASSWORD`).
    -   Check **Use SSL**.
    -   **SMTP Port**: `465`
4.  Click **Save**.

### 5.3. Create the Main Daily Pipeline Job
This job will run your main `Jenkinsfile` once a day to perform the complete ETL process and update the dashboards.

1.  On the Jenkins dashboard, click **New Item**.
2.  Enter the name **`ecommerce-daily-pipeline`**, select **Pipeline**, and click **OK**.
3.  In the configuration page, scroll down to the **Build Triggers** section.
    -   Check **Build periodically**.
    -   In the **Schedule** box, enter `0 8 * * *`. (This means 8:00 AM UTC, which is 10:00 AM Central European Summer Time).
4.  Scroll down to the **Pipeline** section.
    -   **Definition**: `Pipeline script from SCM`.
    -   **SCM**: `Git`.
    -   **Repository URL**: Enter your GitHub repository URL.
    -   **Branch Specifier**: `*/main`.
    -   **Script Path**: `Jenkinsfile` (this is the default and should be correct).
5.  Click **Save**.
6.  You can click **Build Now** to run it once manually and confirm everything works. 

### 5.4. Create the 4-Hourly Transaction Ingestion Job
This is a separate, more frequent job that only ingests new transaction data, as required by the project. It will use a different, simpler `Jenkinsfile`.

2.  **Create the Jenkins Job:**
    -   On the Jenkins dashboard, click **New Item**.
    -   Enter the name **`ingest-transactions-4-hourly`**, select **Pipeline**, and click **OK**.
    -   In **Build Triggers**:
        -   Check **Build periodically**.
        -   In the **Schedule** box, enter `H */4 * * *` (this means "approximately every 4 hours").
    -   In the **Pipeline** section:
        -   **Definition**: `Pipeline script from SCM`.
        -   **SCM**: `Git`.
        -   **Repository URL**: Your GitHub repository URL.
        -   **Branch Specifier**: `*/main`.
        -   **Script Path**: `Jenkinsfile-transactions-only` (This is the crucial step).
    -   Click **Save**.  

### 5.5. Create the Weekly Backup Job

This is a separate job dedicated to system maintenance. It runs on a weekly schedule to create backups of the critical data stores.

1.  **Create the Jenkins Job:**
    -   On the Jenkins dashboard, click **New Item**.
    -   Enter the name **`backup-pipeline`**, select **Pipeline**, and click **OK**.
    -   In the job configuration page, go to the **Build Triggers** section:
        -   Check **Build periodically**.
        -   In the **Schedule** box, enter `0 2 * * 0` (this means "every Sunday at 2 AM").
    -   Scroll down to the **Pipeline** section:
        -   **Definition**: `Pipeline script from SCM`.
        -   **SCM**: `Git`.
        -   **Repository URL**: Your GitHub repository URL.
        -   **Branch Specifier**: `*/main`.
        -   **Script Path**: `Jenkinsfile-backup` (This is the crucial step).
    -   Click **Save**.  

## Step 6: Set Up Metabase & Create Dashboards

### 6.1. Initial Setup

1.  **Access Metabase:** Open `http://localhost:3000` in your browser.
2.  **Create Account:** Follow the on-screen instructions to set up your admin account.
3.  **Connect Database:** When prompted, connect Metabase to your PostgreSQL data warehouse using the credentials from your `.env` file.
    -   **Database type:** `PostgreSQL`
    -   **Display name:** `E-commerce DWH`
    -   **Host:** `postgres-warehouse` (this is the Docker service name)
    -   **Port:** `5432`
    -   **Database name:** The value from your `.env` file (e.g., `ecommerce_dwh`)
    -   **Username:** The value from your `.env` file 
    -   **Password:** The value from your `.env` file.
    -   Click **Save**. Metabase will scan your tables.

### 6.2. Create "Questions" (Charts)

Create the following four questions to answer the business requirements from the Yellow Phase.

#### Question 1: Sales Trend Over Time

1.  Click **+ New** > **Question**.
2.  Start with the **`fct_sales`** table.
3.  Click **Join data** and join with the **`dim_date`** table on `fct_sales.date_id` = `dim_date.date_id`.
4.  In the **Summarize** section:
    -   Metric: `Sum of` > `total_price`.
    -   Group by: `dim_date` > `full_date` (and select `by Day`).
5.  Click **Visualize**. It should default to a **Line Chart**.
6.  Click **Save**, name it `Sales Trend Over Time`, and save it.

#### Question 2: Top 5 Products by Sales

1.  Click **+ New** > **Question**.
2.  Start with **`fct_sales`**.
3.  **Join data** with **`dim_products`** on `fct_sales.product_id` = `dim_products.product_id`.
4.  In **Summarize**:
    -   Metric: `Sum of` > `total_price`.
    -   Group by: `dim_products` > `description`.
5.  Click **Sort**, choose `Sum of total_price`, and select **Descending**.
6.  Click **Filter**, choose **Limit**, and enter `5`.
7.  Click **Visualize**. It should be a **Bar Chart**.
8.  Click **Save**, name it `Top 5 Products by Sales`, and save it.

#### Question 3: Sales by Country

1.  Click **+ New** > **Question**.
2.  Start with **`fct_sales`**.
3.  **Join data** with **`dim_geography`** on `fct_sales.geo_id` = `dim_geography.geo_id`.
4.  In **Summarize**:
    -   Metric: `Sum of` > `total_price`.
    -   Group by: `dim_geography` > `country`.
5.  Click **Visualize**. It should default to a **Map**.
6.  Click **Save**, name it `Sales by Country`, and save it.

#### Question 4: Top 5 Customers by Sales

1.  Click **+ New** > **Question**.
2.  Start with **`fct_sales`**.
3.  In **Summarize**:
    -   Metric: `Sum of` > `total_price`.
    -   Group by: `customer_id`.
4.  Click **Sort**, choose `Sum of total_price`, and select **Descending**.
5.  Click **Filter**, choose **Limit**, and enter `5`.
6.  Click **Visualize**. It should be a **Bar Chart**.
7.  Click **Save**, name it `Top 5 Customers by Sales`, and save it.

### 6.3. Create the Dashboard

1.  Click **+ New** > **Dashboard**.
2.  Name it `E-commerce Sales Overview`.
3.  Click the **+** icon to add your saved questions.
4.  Add all four questions you created.
5.  Drag and resize the charts to create a nice layout.
6.  Click **Save**.   

## Step 7 (Bonus): Running the Streaming Pipeline (Manual Control)

This section describes how to manually start, monitor, and stop the bonus streaming pipeline. This process uses a Python script to simulate a live stream of tweet data, which is then processed by a Spark Streaming job.

### 7.1. Compile the Scala Code (One-time step)

Before running the pipeline for the first time, you must compile the Scala application code. Run this command once in your terminal. You only need to repeat this step if you make changes to the `.scala` files.

```bash
docker compose exec -u root spark-master bash -c "cd /opt/spark/apps/ && sbt clean package"
```

### 7.2. Start the Pipeline

The pipeline consists of two components that you will start from your terminal.
```bash
docker compose exec -d twitter-producer python3 producer.py
```

### Start the Spark Streaming Job

This command starts the Spark job that reads from Kafka, processes the data, and writes to PostgreSQL. This command will occupy your current terminal window and display live logs.

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit   --class KafkaBatchProcessor   --master spark://spark-master:7077   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0   --jars /opt/spark/jars-custom/postgresql-42.7.3.jar   /opt/spark/apps/target/scala-2.12/ecommerce-pipeline-spark-jobs_2.12-1.0.jar
```

### 7.3. Monitor and Check Results

While the Spark job is running, you can open new terminal windows to monitor the system:

- **Check Spark UI**: Go to `http://localhost:8081`. You should see *Kafka Batch Processor* in the "Running Applications" table.

- **"Spy" on Kafka Messages**: To see the data flowing into Kafka in real-time.

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic twitter_trends
```

### 7.4. Stop the Pipeline

When you are finished, you need to stop both processes.

- **Stop the Spark Job**: Go to the terminal where the `spark-submit` command is running and press `Ctrl + C`.

- **Stop the Data Producer**: Run the following command to find and stop the Python script running inside the `twitter-producer` container.

```bash
docker compose exec twitter-producer bash -c 'kill $(ps -ef | grep "[p]roducer.py" | awk "{print \$2}")'
```

### 3. Create the Metabase "Question" (Chart)

After you have run the `START` action and let the pipeline run for a few minutes to collect and process data, you can visualize the results.

1.  In Metabase, click **`+ New`** > **`Question`**.
2.  Select your database (e.g., `E-commerce DWH`) and the new **`trending_products`** table.
3.  You will see a table with the trending products. Click the **`Visualization`** button at the bottom and select the **`Bar`** chart icon.
4.  Click **`Save`**. Name the question `Live Twitter Trends` and save it.

### 4. Create the Final Dashboard

1.  On the main Metabase page, click **`+ New`** > **`Dashboard`**.
2.  Name your new dashboard **`Bonus - Live Trends`**.
3.  Click the **`+`** icon to add your saved `Live Twitter Trends` question to the dashboard.
4.  Resize the chart as needed.
5.  **Set up Auto-Refresh:** To make the dashboard "live," click the **Clock icon (🕒)** in the top-right corner of the dashboard and set it to refresh **every 5 minutes**.
6.  Click **`Save`** to save the dashboard.

