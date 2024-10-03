# Docker ECR

Repo:
604815197344.dkr.ecr.us-east-1.amazonaws.com/604815197344/testpyimg

```bash
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 604815197344.dkr.ecr.us-east-1.amazonaws.com

aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws

docker build -t testpyimg .
docker run --name testpyimg_ct testpyimg:latest
docker rm testpyimg_ct
docker rmi testpyimg

ID=public.ecr.aws/o1j4w8l5/604815197344/testpyimg_pub:latest
ID=604815197344.dkr.ecr.us-east-1.amazonaws.com/604815197344/testpyimg:latest
docker tag testpyimg:latest $ID
docker push $ID
```
