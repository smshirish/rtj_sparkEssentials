##Commands needed on the www.rock-the-jvm.com course

echo "Starting Idea ij"
intellij-idea-community


##course related commands
echo  "Starting docker as service "
sudo systemctl start docker

##starting the postgres db
cd ~/workspace//spark/spark-essentials-start/ || { echo "Failure"; exit 1; }
sudo docker-compose up

##open another shell and run below
##starting the postgres db
cd ~/workspace//spark/spark-essentials-start/ || { echo "Failure"; exit 1; }
sudo ~/workspace//spark/spark-essentials-start/psql.sh

##open another shell to start spark engine
cd ~/workspace//spark/spark-essentials-start/spark-cluster || { echo "Failure"; exit 1; }
sudo docker-compose up --scale spark-worker=3
