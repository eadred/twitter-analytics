. ./set-vars.ps1

$src="../../ta-common/target/ta-common-bundled-1.0-SNAPSHOT.jar"
$dest=$jarloc + "/ta-common-bundled-1.0-SNAPSHOT.jar"

gsutil cp $src $dest

$src="../../ta-spark-sentiment-deps/target/ta-spark-sentiment-deps-bundled-1.0-SNAPSHOT.jar"
$dest=$jarloc + "/ta-spark-sentiment-deps-bundled-1.0-SNAPSHOT.jar"

gsutil cp $src $dest

$src="../../ta-spark-sentiment/target/ta-spark-sentiment-1.0-SNAPSHOT.jar"
$dest=$jarloc + "/ta-spark-sentiment-1.0-SNAPSHOT.jar"

gsutil cp $src $dest