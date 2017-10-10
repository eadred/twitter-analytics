gcloud container clusters get-credentials twitter-analytics --zone europe-west1-b --project analytics-174711

kubectl run ta-webapp --image=eu.gcr.io/analytics-174711/ta-webapp --port=4567

kubectl expose deployment ta-webapp --type="LoadBalancer"

kubectl get service ta-webapp

# To delete:
# kubectl delete deployment ta-webapp