// Jenkinsfile for the E-commerce Batch Pipeline (Final Version for Local Testing)

pipeline {
    agent any

    stages {

        // Stage 1: Get the source code from Git. This is the first step for a production pipeline.
        stage('Checkout SCM') {
            steps {
                checkout scm
            }
        }

        // Stage 2: Compile the Scala Spark application into a JAR file.
        stage('Compile Scala Code') {
            steps {
                echo "--- Compiling Spark application ---"
                sh 'docker exec -u root spark-master bash -c "cd /opt/spark/apps/ && sbt package"'
            }
        }

        // Stage 3: Generate a fresh batch of random data for the pipeline run.
        stage('Generate Data') {
            steps {
                sh 'echo "--- Generating daily dimension files ---"'
                sh 'python3 scripts/data_generator.py --type dimensions'

                sh 'echo "--- Generating a batch of transaction files ---"'
                sh 'python3 scripts/data_generator.py --type transactions'
            }
        }

        // Stage 4: Upload all generated data to the HDFS Data Lake.
        stage('Upload Data to HDFS') {
            steps {
                sh '''
                    echo "--- Uploading all files to HDFS ---"
                    docker exec namenode hdfs dfs -mkdir -p /user/spark/raw_dimensions
                    docker exec namenode hdfs dfs -mkdir -p /user/spark/raw_transactions

                    cat generated_data/countries.csv | docker exec -i namenode hdfs dfs -put -f - /user/spark/raw_dimensions/countries.csv
                    cat generated_data/product_info.csv | docker exec -i namenode hdfs dfs -put -f - /user/spark/raw_dimensions/product_info.csv

                    for f in generated_data/invoice_*.txt; do
                        if [ -f "$f" ]; then
                            filename=$(basename "$f")
                            cat "$f" | docker exec -i namenode hdfs dfs -put -f - "/user/spark/raw_transactions/$filename"
                        fi
                    done
                '''
            }
        }
        
        // Stage 5: Run Spark jobs to create the Bronze layer tables sequentially.
        stage('Process Staging (Bronze) Tables') {
            steps {
                echo "--- Running Country Dimension Spark Job ---"
                sh 'docker exec spark-master bash -c "/opt/spark/bin/spark-submit --class ProcessCountries --master spark://spark-master:7077 /opt/spark/apps/target/scala-2.12/ecommerce-pipeline-spark-jobs_2.12-1.0.jar"'
                
                echo "--- Running Product Dimension Spark Job ---"
                sh 'docker exec spark-master bash -c "/opt/spark/bin/spark-submit --class ProcessProducts --master spark://spark-master:7077 /opt/spark/apps/target/scala-2.12/ecommerce-pipeline-spark-jobs_2.12-1.0.jar"'

                echo "--- Running Transaction Ingestion Spark Job ---"
                sh 'docker exec spark-master bash -c "/opt/spark/bin/spark-submit --class IngestTransactions --master spark://spark-master:7077 /opt/spark/apps/target/scala-2.12/ecommerce-pipeline-spark-jobs_2.12-1.0.jar"'
            }
        }

        // Stage 6: Run the main ETL job to build the Gold Layer Data Warehouse.
        stage('Load Data Warehouse (Gold Layer)') {
            steps {
                script {
                    sh '''
                        echo "Running Spark job to load the Gold Layer (Data Warehouse)..."
                        
                        # PostgreSQL credentials are already provided via Docker Compose environment variables
                        docker exec spark-master bash -c '
                            /opt/spark/bin/spark-submit \
                                --class LoadDWH \
                                --master spark://spark-master:7077 \
                                --jars /opt/spark/jars-custom/postgresql-42.7.3.jar \
                                /opt/spark/apps/target/scala-2.12/ecommerce-pipeline-spark-jobs_2.12-1.0.jar
                        '
                    '''
                }
            }
        }
    }

    // This 'post' block runs after all stages are complete.
    post {
        always {
            script {
                // First, check for and send the business logic alert.
                echo "Checking for business logic alerts..."
                sh 'python3 scripts/send_alert_email.py'

                // Then, send the standard pipeline status notification.
                echo "Sending pipeline status email..."
                mail to: env.NOTIFICATION_EMAIL,
                    subject: "${currentBuild.result}: Jenkins Pipeline - ${env.JOB_NAME} [${env.BUILD_NUMBER}]",
                    body: "The pipeline finished with status: ${currentBuild.result}. For details, see the log at: ${env.BUILD_URL}"
            }
        }
    }
}
