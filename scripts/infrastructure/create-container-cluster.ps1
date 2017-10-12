gcloud beta container `
--project "analytics-174711" `
clusters create "twitter-analytics" `
--zone "europe-west1-b" `
--username="admin" `
--cluster-version "1.7.6-gke.1" `
--machine-type "n1-standard-1" `
--image-type "COS" `
--disk-size "10" `
--scopes "https://www.googleapis.com/auth/compute","https://www.googleapis.com/auth/devstorage.read_only","https://www.googleapis.com/auth/bigquery","https://www.googleapis.com/auth/logging.write","https://www.googleapis.com/auth/monitoring","https://www.googleapis.com/auth/pubsub","https://www.googleapis.com/auth/servicecontrol","https://www.googleapis.com/auth/service.management.readonly","https://www.googleapis.com/auth/trace.append" `
--num-nodes "1" `
--network "default" `
--enable-cloud-logging `
--no-enable-cloud-monitoring `
--subnetwork "default" `
--enable-legacy-authorization `
--no-enable-network-policy