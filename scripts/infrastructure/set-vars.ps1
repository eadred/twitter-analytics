$env:PYTHONIOENCODING='utf-8'

$project="analytics-174711"
$table="tweets"
$region="europe-west1"
$zone="europe-west1-b"
$dataset="camp_exercise"
$topic="tweets"
$spark_cluster="spark-sentiment"
$container_cluster="twitter-analytics"
$jarloc="gs://twitter-analytics/spark"
$windowsizesecs=60