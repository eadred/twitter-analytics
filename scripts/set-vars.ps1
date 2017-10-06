$env:PYTHONIOENCODING='utf-8'

$project="analytics-174711"
$table="tweets"
$region="europe-west1"
$dataset="camp_exercise"
$topic="tweets"
$cluster="spark-sentiment"
$jarloc="gs://eadred-dataflow/spark"
$windowsizesecs=60