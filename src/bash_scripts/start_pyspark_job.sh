#!/bin/bash
gcloud dataproc jobs submit pyspark gs://hdfs-cluster2/notebooks/jupyter/hdfs-to-cassandra.py  --region=europe-central2 --cluster=cluster-hdfs
