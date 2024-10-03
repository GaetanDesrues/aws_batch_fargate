# AWS Batch with Fargate Elastic Compute

Un example de traitement par lot avec AWS Batch.

Une fonction lambda est déclenchée avec un cron schedule.
Elle partionne les données (tampon sur s3) et lance les jobs.
