. ./set-vars.ps1

$mainjar=$jarloc + "/ta-spark-sentiment-1.0-SNAPSHOT.jar"
$sentimentdepsjar=$jarloc + "/ta-spark-sentiment-deps-bundled-1.0-SNAPSHOT.jar"
$commonjar=$jarloc + "/ta-common-bundled-1.0-SNAPSHOT.jar"

gcloud dataproc jobs submit spark `
	--cluster $cluster `
	--jars=$mainjar,$sentimentdepsjar,$commonjar `
	--class=com.zuhlke.ta.prototype.solutions.gc.SparkSentiment	`
	--region=europe-west1 `
	-- $project `
	$topic `
	$dataset `
	$table `
	$windowsizesecs