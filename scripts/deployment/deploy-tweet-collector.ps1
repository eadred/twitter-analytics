gcloud container clusters get-credentials twitter-analytics --zone europe-west1-b --project analytics-174711

kubectl create -f tweet-collector-deployment.yaml

# Get details
# kubectl get deployment tweet-collector-deployment
# kubectl get pods -l app=tweet-collector

# Access shell on containers
# kubectl exec $(kubectl get pods -l app=tweet-collector -o jsonpath='{.items[*].metadata.name}') -c tweet-collector -it -- /bin/bash

# To delete:
# kubectl delete deployment tweet-collector-deployment