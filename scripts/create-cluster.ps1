. ./set-vars.ps1

gcloud beta dataproc clusters create $cluster `
    --region $region `
    --subnet default `
    --master-machine-type n1-standard-2 `
    --master-boot-disk-size 10 `
    --num-workers 3 `
    --worker-machine-type n1-standard-2 `
    --worker-boot-disk-size 10 `
    --image-version 1.2 `
    --scopes 'https://www.googleapis.com/auth/cloud-platform'
    