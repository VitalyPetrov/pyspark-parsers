echo "--------------------------------------------------------"
echo "usage: ./submit-parser.sh src/<script-to-launch>"
echo "--------------------------------------------------------"

export PYTHONIOENCODING=utf8
export PYTHONPATH=$(pwd)
export SPARK_CLASSPATH=$(pwd)/jars/

spark-submit  --master yarn \
              --py-files libs.zip \
              --executor-memory 2G \
              --driver-memory 2G \
              --num-executors 2 \
              --executor-cores 1 \
              --conf spark.yarn.appMasterEnv.PYTHONIOENCODING=utf8 \
              --packages com.databricks:spark-csv_2.10:1.5.0\
               $1