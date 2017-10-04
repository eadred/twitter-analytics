. ./set-vars.ps1

$fqtn=$dataset + "." + $table
bq mk -t $fqtn tweets_schema.json