# MATDF

## Runnig Shoveler in the docker dev box

1. Do git pull source codes in the dev box
2. Build docker image like below and run docker container

>aws.properties file should be also be in project directory

```
# Build docker image
sudo docker build -t dataflow/shoveler .

# Run docker container to run shoveler
sudo docker run -d dataflow/shoveler

# check shoveler console output log
sudo docker logs CONTAINER_ID 

# stop running
sudo docker stop CONTAINER_ID
```
