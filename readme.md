# eCommerce Behavior Data Analysis with Spark
*Notes: This Spark job is intended to be deployed to Cloud Dataproc in [Google Cloud Platform](https://cloud.google.com/) (GCP).*

### 1. Dataset
The dataset used for this analysis comes from Kaggle's [eCommerce behavior data from multi category store](https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store). The total size of this dataset is over 50GB (including the [additional archive](https://drive.google.com/drive/folders/1Nan8X33H8xrXS5XhCKZmSpClFTCJsSpE) provided in Google Drive).
#### Prepare Dataset
Run the following command in a [Compute Engine](https://cloud.google.com/compute) instance with enough storage space (recommended), or in your local machine if you have [Cloud SDK](https://cloud.google.com/sdk) installed. Replace `<your-gcs-bucket-name>` with your [Google Cloud Storage](https://cloud.google.com/storage) (GCS) bucket name. All the files in this dataset will be pushed to your GCS bucket.
```
./prepare_dataset.sh <your-gcs-bucket-name>
```
If you run into errors, make sure have [`kaggle`](https://www.kaggle.com/docs/api) and [`gdown`](https://pypi.org/project/gdown/) command installed.

### 2. Build Project
Simply run the follow command to build this project:
```
sbt clean assembly
```
Your Jar file will be found under directory `target/scala-2.11/`

### 3. Upload Jar to GCS
Run the following command in [Cloud Shell](https://cloud.google.com/shell), Compute Engine, or your local machine with Cloud SDK:
```
gsutil cp /path/to/jarfile/eCommerce-Behavior-Data-Analysis-assembly.jar gs://<your-gcs-bucket-name>/jar/
```

### 4. Create a Cloud Dataproc Cluster
Run the following command in Cloud Shell, Compute Engine, or your local machine with Cloud SDK:
```
gcloud dataproc clusters <your-cluster-name> --source cluster.yaml
```
Replace `<your-cluster-name>` with a cluster name of your choice.

### 5. Submit Spark Job to Dataproc Cluster
Run the following command in Cloud Shell, Compute Engine, or your local machine with Cloud SDK:
```
gcloud dataproc jobs submit spark \
    --cluster=<my-cluster-name> \
    --region=asia-southeast1 \
    --class com.ecommerce.project.Entrypoint \
    --jar gs://<your-gcs-bucket-name>/jar/eCommerce-Behavior-Data-Analysis-assembly.jar \
    --async \
    -- <your-gcs-bucket-name>
```