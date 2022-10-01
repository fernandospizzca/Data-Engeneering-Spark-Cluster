spark-submit \
--deploy-mode client \
--master spark://192.168.56.109:7077 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.executor.extrajavaoptions="-XX:-UseGCOverheadLimit -verbose:gc-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
--py-files=joinBases.py,ETLVac.py,ETLCovid.py main.py > log_job.txt 2>&1