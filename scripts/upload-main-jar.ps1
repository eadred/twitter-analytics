. ./set-vars.ps1

$src="../ta-spark-sentiment/target/ta-spark-sentiment-1.0-SNAPSHOT.jar"
$dest=$jarloc + "/ta-spark-sentiment-1.0-SNAPSHOT.jar"

gsutil cp $src $dest