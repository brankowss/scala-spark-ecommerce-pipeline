pipeline {
    agent any

    environment {
        HDFS_DIM_DIR   = "/user/spark/raw_dimensions"
        WORKSPACE_DIR  = "${env.WORKSPACE}/generated_data"
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

                echo "--- Uploading all data to HDFS ---"
                sh '''
                    echo "--- Uploading all files to HDFS ---"
                    # Create BOTH directories at once
                    docker-compose exec namenode sh -c 'hdfs dfs -mkdir -p /user/spark/raw_dimensions /user/spark/raw_transactions'
                    
                    # Upload dimension files
                    docker-compose exec namenode sh -c 'hdfs dfs -put -f /tmp/data_in/countries.csv /user/spark/raw_dimensions/'
                    docker-compose exec namenode sh -c 'hdfs dfs -put -f /tmp/data_in/product_info.csv /user/spark/raw_dimensions/'
                    
                    # Upload transaction files directly to the correct location 
                    docker-compose exec namenode sh -c 'hdfs dfs -put -f /tmp/data_in/invoice_*.txt /user/spark/raw_transactions/'
                '''

                echo "--- HDFS upload complete ---"

                echo "--- Verifying uploaded files ---"
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
                     body: "The pipeline finished with status: ${currentBuild.result}. For details, see the log at: ${env.BUILD_URL}"
            }
        }
    }
}
