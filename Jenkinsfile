pipeline {
    agent any

    environment {
        HDFS_DIM_DIR   = "/user/spark/raw_dimensions"
        HDFS_TRANS_DIR = "/user/spark/raw_transactions"
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

                echo "--- Uploading all files to HDFS ---"
                sh '''
                    # Create HDFS directories if they don't exist
                    docker exec namenode hdfs dfs -mkdir -p ${HDFS_DIM_DIR}
                    docker exec namenode hdfs dfs -mkdir -p ${HDFS_TRANS_DIR}

                    # Upload dimension CSV files
                    docker exec -i namenode hdfs dfs -put -f ${WORKSPACE_DIR}/countries.csv ${HDFS_DIM_DIR}/countries.csv
                    docker exec -i namenode hdfs dfs -put -f ${WORKSPACE_DIR}/product_info.csv ${HDFS_DIM_DIR}/product_info.csv

                    # Upload all transaction TXT files (preserve filenames)
                    for f in ${WORKSPACE_DIR}/invoice_*.txt; do
                        docker exec -i namenode hdfs dfs -put -f "$f" ${HDFS_TRANS_DIR}/$(basename $f)
                    done

                    echo "--- HDFS upload complete ---"
                '''

                echo "--- Verifying uploaded files ---"
                sh 'docker exec namenode hdfs dfs -ls ${HDFS_DIM_DIR}/'
                sh 'docker exec namenode hdfs dfs -ls ${HDFS_TRANS_DIR}/'
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
