gcloud container clusters get-credentials twitter-analytics --zone europe-west1-b --project analytics-174711

kubectl create -f couchbase-service.yaml
kubectl create -f webapp-service.yaml
kubectl create -f couchbase-deployment.yaml
kubectl create -f webapp-deployment.yaml


# kubectl expose deployment webapp-deployment --port=80 --target-port=4567 --type="LoadBalancer"

# Get details
# kubectl get deployment webapp-deployment
# kubectl get pods -l app=webapp
# kubectl get service webapp-service

# Access shell on containers
# kubectl exec $(kubectl get pods -l app=webapp -o jsonpath='{.items[*].metadata.name}') -c ta-webapp -it -- /bin/bash
# kubectl exec $(kubectl get pods -l app=couchbase -o jsonpath='{.items[*].metadata.name}') -c ta-couchbase -it -- /bin/bash

# To delete:
# kubectl delete deployment webapp-deployment
# kubectl delete deployment couchbase-deployment
# kubectl delete service webapp-service