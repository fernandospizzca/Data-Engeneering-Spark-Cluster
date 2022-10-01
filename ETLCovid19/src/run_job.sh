mkdir /tmp/spark-events
chmod 777 /tmp/spark-events

spark-submit \
--deploy-mode client \
--master spark://192.168.1.10:7077 \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.standalone.submit.waitAppCompletion=false \
--conf "spark.executor.extrajavaoptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
--py-files=ETLVac.py,ETLCovid.py,joinBases.py main.py > log_job.txt 2>&1