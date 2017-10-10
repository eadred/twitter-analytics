gcloud container clusters get-credentials twitter-analytics --zone europe-west1-b --project analytics-174711

kubectl run tweet-collector --image=eu.gcr.io/analytics-174711/tweet-collector

kubectl get service tweet-collector

# To delete:
# kubectl delete deployment tweet-collector