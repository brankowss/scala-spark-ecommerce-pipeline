// Jenkinsfile 

pipeline {
    // This tells Jenkins to run the pipeline on any available agent (node).
    agent any

    // Define environment variables for clarity and reusability.
    environment {
        HDFS_DIM_DIR   = "/user/spark/raw_dimensions"
        WORKSPACE_DIR  = "${env.WORKSPACE}/generated_data"
    }

    stages {

        // STAGE 0: Get the source code from Git. This is the first step for a production pipeline.
        stage('Checkout SCM') {
            steps {
                // This built-in step clones/pulls the repository that is configured in the Jenkins job UI.
                checkout scm
            }
        }

        // Stage 1: Generate all necessary data files for a test run.
        stage('Generate & Upload All Data to HDFS') {
            steps {
                echo "--- Generating daily dimension files ---"
                sh 'python3 scripts/data_generator.py --type dimensions'

                echo "--- Generating a small batch of transaction files ---"
                sh '''
                    # Run transaction generator for a short period and then stop it.
                    python3 scripts/data_generator.py --type transactions &
                    sleep 10
                    pkill -f data_generator.py || true
                '''

                echo "--- Uploading all data to HDFS ---"
                sh '''
                    # Create HDFS directories if they don't exist.
                    docker exec namenode hdfs dfs -mkdir -p /user/spark/raw_dimensions
                    docker exec namenode hdfs dfs -mkdir -p /user/spark/raw_transactions/data_in

                    # Upload dimension CSV files by streaming them from the workspace into HDFS.
                    # The '-f' flag overwrites the files if they already exist.
                    cat ${WORKSPACE}/generated_data/countries.csv | docker exec -i namenode hdfs dfs -put -f - /user/spark/raw_dimensions/countries.csv
                    cat ${WORKSPACE}/generated_data/product_info.csv | docker exec -i namenode hdfs dfs -put -f - /user/spark/raw_dimensions/product_info.csv

                    # Upload all generated transaction TXT files into the target directory.
                    for f in ${WORKSPACE}/generated_data/invoice_*.txt; do
                        cat "$f" | docker exec -i namenode hdfs dfs -put -f - /user/spark/raw_transactions/data_in/
                    done

                    echo "--- HDFS upload complete ---"
                '''

                echo "--- Verifying uploaded files ---"
                sh 'docker exec namenode hdfs dfs -ls /user/spark/raw_dimensions/'
                sh 'docker exec namenode hdfs dfs -ls /user/spark/raw_transactions/data_in/'
            }
        }

        // Stage 2: Run Spark jobs to create the Bronze layer tables in parallel to save time.
        stage('Process Staging (Bronze) Tables') {
            parallel {
                stage('Process Transactions') {
                    steps {
                        sh '''
                            echo "--- Running Transaction Ingestion Spark Job ---"
                            docker exec spark-master bash -c "spark-submit \
                                --class IngestTransactions \
                                --master spark://spark-master:7_077 \
                                /opt/bitnami/spark/apps/target/scala-2.12/ecommerce-pipeline-spark-jobs_2.12-1.0.jar"
                        '''
                    }
                }
                stage('Process Countries') {
                    steps {
                        sh '''
                            echo "--- Running Country Dimension Spark Job ---"
                            docker exec spark-master bash -c "spark-submit \
                                --class ProcessCountries \
                                --master spark://spark-master:7077 \
                                /opt/bitnami/spark/apps/target/scala-2.12/ecommerce-pipeline-spark-jobs_2.12-1.0.jar"
                        '''
                    }
                }
                stage('Process Products') {
                    steps {
                        sh '''
                            echo "--- Running Product Dimension Spark Job ---"
                            docker exec spark-master bash -c "spark-submit \
                                --class ProcessProducts \
                                --master spark://spark-master:7077 \
                                /opt/bitnami/spark/apps/target/scala-2.12/ecommerce-pipeline-spark-jobs_2.12-1.0.jar"
                        '''
                    }
                }
            }
        }

        // Stage 3: Run the main ETL job that joins the bronze tables and loads the final Data Warehouse.
        stage('Load Data Warehouse (Gold Layer)') {
            steps {
                sh '''
                    echo "--- Running Gold Layer Load Spark Job ---"
                    docker exec spark-master bash -c "spark-submit \
                        --class LoadDWH \
                        --master spark://spark-master:7077 \
                        --jars /opt/bitnami/spark/jars/postgresql-42.5.0.jar \
                        /opt/bitnami/spark/apps/target/scala-2.12/ecommerce-pipeline-spark-jobs_2.12-1.0.jar"
                '''
            }
        }
    }

    // This 'post' block runs after all stages are complete.
    post {
        always {
            echo "Pipeline finished. Sending status email..."
            // Securely load the credential with the ID 'NOTIFICATION_EMAIL'.
            withCredentials([string(credentialsId: 'NOTIFICATION_EMAIL', variable: 'RECIPIENT_EMAIL')]) {
                // The built-in 'currentBuild.result' variable will be 'SUCCESS' or 'FAILURE'.
                mail to: RECIPIENT_EMAIL,
                     subject: "${currentBuild.result}: Jenkins Pipeline - ${env.JOB_NAME} [${env.BUILD_NUMBER}]",
                     body: "The pipeline finished with status: ${currentBuild.result}. For details, see the log at: ${env.BUILD_URL}"
            }
        }
    }
}