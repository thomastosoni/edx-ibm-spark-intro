Submit Spark jobs to Kubernetes

Set up Apache Spark POD with:
`k apply -f ../spark/pod_spark.yaml -n default`

Set up Kubernetes RBAC Authorization with:
`k apply -f rbac.yaml -n default`

Run basic SparkPi job with:
```
./bin/spark-submit \
--master k8s://http://127.0.0.1:8001 \
--deploy-mode cluster \
--name spark-pi \
--class org.apache.spark.examples.SparkPi \
--conf spark.executor.instances=3 \
--conf spark.kubernetes.container.image=romeokienzler/spark-py:3.1.2 \
--conf spark.kubernetes.executor.limit.cores=1 \
local:///opt/spark/examples/jars/spark-examples_2.12-3.1.2.jar \
10
```
