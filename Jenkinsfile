pipeline {
    agent any

    environment {
        HDFS_DIM_DIR   = "/user/spark/raw_dimensions"
        HDFS_TRANS_DIR = "/user/spark/raw_transactions"
        WORKSPACE_DIR  = "${env.WORKSPACE}/generated_data" // Jenkins workspace path for generated data
    }

    stages {

        stage('Checkout SCM') {
            steps {
                checkout scm
            }
        }

        stage('Generate & Upload All Data to HDFS') {
            steps {
                echo "--- Generating daily dimension files ---"
                sh 'python3 scripts/data_generator.py --type dimensions'

                echo "--- Generating a small batch of transaction files ---"
                sh '''
                    python3 scripts/data_generator.py --type transactions &
                    sleep 10
                    pkill -f data_generator.py || true
                '''

                echo "--- Uploading all files to HDFS ---"
                sh '''
                    # Create HDFS directories
                    docker exec namenode hdfs dfs -mkdir -p /user/spark/raw_dimensions
                    docker exec namenode hdfs dfs -mkdir -p /user/spark/raw_transactions

                    # Stream dimension CSV files from Jenkins workspace to HDFS
                    cat generated_data/countries.csv | docker exec -i namenode hdfs dfs -put -f - /user/spark/raw_dimensions/countries.csv
                    cat generated_data/product_info.csv | docker exec -i namenode hdfs dfs -put -f - /user/spark/raw_dimensions/product_info.csv

                    # Loop through transaction files and stream each one individually
                    for f in generated_data/invoice_*.txt; do
                        if [ -f "$f" ]; then
                            filename=$(basename "$f")
                            echo "Uploading $filename to HDFS..."
                            cat "$f" | docker exec -i namenode hdfs dfs -put -f - "/user/spark/raw_transactions/$filename"
                        fi
                    done

                    echo "--- HDFS upload complete ---"
                '''

                echo "--- Verifying uploaded files in HDFS ---"
                sh 'docker exec namenode hdfs dfs -ls /user/spark/raw_dimensions/'
                sh 'docker exec namenode hdfs dfs -ls /user/spark/raw_transactions/'
            }
        }

        stage('Process Staging (Bronze) Tables') {
            parallel {
                stage('Process Transactions') {
                    steps {
                        sh '''
                            echo "--- Running Transaction Ingestion Spark Job ---"
                            docker exec spark-master bash -c "spark-submit \
                                --class IngestTransactions \
                                --master spark://spark-master:7077 \
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

    post {
        always {
            echo "Pipeline finished. Sending status email..."
            withCredentials([string(credentialsId: 'NOTIFICATION_EMAIL', variable: 'RECIPIENT_EMAIL')]) {
                mail to: RECIPIENT_EMAIL,
                     subject: "${currentBuild.result}: Jenkins Pipeline - ${env.JOB_NAME} [${env.BUILD_NUMBER}]",
                     body: "The pipeline finished with status: ${currentBuild.result}. Log: ${env.BUILD_URL}"
            }
        }
    }
}
