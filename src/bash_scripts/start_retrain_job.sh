#!/bin/bash
gcloud dataproc jobs submit pyspark gs://hdfs-cluster2/notebooks/jupyter/retrain_model.py --region=europe-central2 --cluster=cluster-hdfs
