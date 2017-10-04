. ./set-vars.ps1

gcloud beta dataproc clusters create $cluster `
    --region $region `
    --subnet default `
    --single-node `
    --master-machine-type n1-standard-4 `
    --master-boot-disk-size 30 `
    --image-version 1.2 `
    --scopes 'https://www.googleapis.com/auth/cloud-platform'
    