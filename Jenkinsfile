// Jenkinsfile for the E-commerce Batch Pipeline

pipeline {
    agent any

    stages {

        // Stage 0: Get the source code from Git. This is the first step for a production pipeline.
        stage('Checkout SCM') {
            steps {
                // This built-in step clones/pulls the repository that is configured in the Jenkins job UI.
                checkout scm
            }
        }

        // Stage 1: Generate a fresh batch of random data for the pipeline run.
        stage('Generate Data') {
            steps {
                sh 'echo "--- Generating daily dimension files ---"'
                sh 'python3 scripts/data_generator.py --type dimensions'

                sh 'echo "--- Generating a batch of transaction files ---"'
                // For production, we run the generator in a continuous loop and stop it after a set time.
                // The '|| true' ensures the pipeline doesn't fail if pkill finds no process to stop.
                sh '''
                    python3 scripts/data_generator.py --type transactions &
                    sleep 30
                    pkill -f data_generator.py || true
                '''

                // --- TESTING THE EMAIL ALERT ---
                // To reliably demonstrate the EP-7 business alert feature, the block above can be
                // commented out, and the single line below can be uncommented. This will run
                // the data generator in a special test mode that guarantees an outlier is created.
                //
                // sh 'python3 scripts/data_generator.py --type transactions --test-mode outlier'
            }
        }

        // Stage 2: Upload all generated data to the HDFS Data Lake.
        stage('Upload Data to HDFS') {
            steps {
                sh '''
                    echo "--- Uploading all files to HDFS ---"
                    # Create HDFS directories if they don't exist.
                    docker exec namenode hdfs dfs -mkdir -p /user/spark/raw_dimensions
                    docker exec namenode hdfs dfs -mkdir -p /user/spark/raw_transactions

                    # Stream all generated files from the Jenkins workspace to HDFS.
                    cat generated_data/countries.csv | docker exec -i namenode hdfs dfs -put -f - /user/spark/raw_dimensions/countries.csv
                    cat generated_data/product_info.csv | docker exec -i namenode hdfs dfs -put -f - /user/spark/raw_dimensions/product_info.csv

                    # Loop through transaction files and stream each one individually.
                    for f in generated_data/invoice_*.txt; do
                        if [ -f "$f" ]; then
                            filename=$(basename "$f")
                            cat "$f" | docker exec -i namenode hdfs dfs -put -f - "/user/spark/raw_transactions/$filename"
                        fi
                    done
                '''
            }
        }

        // Stage 3: Run Spark jobs to create the Bronze layer tables in parallel to save time.
        stage('Process Staging (Bronze) Tables') {
            parallel {
                stage('Process Transactions') {
                    steps {
                        sh 'docker exec spark-master bash -c "spark-submit --class IngestTransactions --master spark://spark-master:7077 /opt/bitnami/spark/apps/target/scala-2.12/ecommerce-pipeline-spark-jobs_2.12-1.0.jar"'
                    }
                }
                stage('Process Countries') {
                    steps {
                        sh 'docker exec spark-master bash -c "spark-submit --class ProcessCountries --master spark://spark-master:7077 /opt/bitnami/spark/apps/target/scala-2.12/ecommerce-pipeline-spark-jobs_2.12-1.0.jar"'
                    }
                }
                stage('Process Products') {
                    steps {
                        sh 'docker exec spark-master bash -c "spark-submit --class ProcessProducts --master spark://spark-master:7077 /opt/bitnami/spark/apps/target/scala-2.12/ecommerce-pipeline-spark-jobs_2.12-1.0.jar"'
                    }
                }
            }
        }

        // Stage 4: Run the main ETL job to build the Gold layer (Data Warehouse).
        stage('Load Data Warehouse (Gold Layer)') {
            steps {
                sh 'docker exec spark-master bash -c "spark-submit --class LoadDWH --master spark://spark-master:7077 --jars /opt/bitnami/spark/jars/postgresql-42.5.0.jar /opt/bitnami/spark/apps/target/scala-2.12/ecommerce-pipeline-spark-jobs_2.12-1.0.jar"'
            }
        }
    }

    // This 'post' block runs after all stages are complete.
    post {
        always {
            withCredentials([
                string(credentialsId: 'NOTIFICATION_EMAIL', variable: 'NOTIFICATION_EMAIL'),
                string(credentialsId: 'JENKINS_SENDER_EMAIL', variable: 'JENKINS_SENDER_EMAIL'),
                string(credentialsId: 'JENKINS_EMAIL_APP_PASSWORD', variable: 'JENKINS_EMAIL_APP_PASSWORD')
            ]) {
                // First, check for and send the business logic alert.
                echo "Checking for business logic alerts..."
                sh 'python3 scripts/send_alert_email.py'

                // Then, send the standard pipeline status notification.
                echo "Sending pipeline status email..."
                mail to: NOTIFICATION_EMAIL,
                    subject: "${currentBuild.result}: Jenkins Pipeline - ${env.JOB_NAME} [${env.BUILD_NUMBER}]",
                    body: "The pipeline finished with status: ${currentBuild.result}. For details, see the log at: ${env.BUILD_URL}"
            }
        }
    }
}

