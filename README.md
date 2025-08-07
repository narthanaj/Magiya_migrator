# For mysql_v1
cat /home/narthanaj/Desktop/Magiya_Migrator/DB/magiyav1.sql | docker exec -i magiya_mysql_v1 mysql -uroot -ppassword magiya_v1

# For mysql_v2
cat /home/narthanaj/Desktop/Magiya_Migrator/DB/magiyav2.sql | docker exec -i magiya_mysql_v2 mysql -uroot -ppassword magiya_v2


# Check tables in magiya_v1
docker exec -it magiya_mysql_v1 mysql -uroot -ppassword magiya_v1 -e "SHOW TABLES;"

# Check tables in magiya_v2  
docker exec -it magiya_mysql_v2 mysql -uroot -ppassword magiya_v2 -e "SHOW TABLES;"



# Build the Docker image
docker build -t magiya-migrator .

# Run interactively
docker run -it --rm \
  -v $(pwd)/logs:/app/logs \
  -v $(pwd)/backup:/app/backup \
  -v $(pwd)/.env:/app/.env:ro \
  magiya-migrator

# Using docker-compose
docker-compose up -d
docker-compose exec migrator python main.py

# Run specific scripts
docker-compose exec migrator python backup_v1.py
docker-compose exec migrator python test_migration.py

# View logs
docker-compose logs -f migrator

# Stop and remove
docker-compose down


cat /home/narthanaj/Desktop/Magiya_Migrator/DB/magiyav1.sql | docker exec -i magiya_mysql_v1 mysql -uroot -ppassword magiya_v1 && \
cat /home/narthanaj/Desktop/Magiya_Migrator/DB/magiyav2.sql | docker exec -i magiya_mysql_v2 mysql -uroot -ppassword magiya_v2 && \
docker exec -it magiya_mysql_v1 mysql -uroot -ppassword magiya_v1 -e "SHOW TABLES;" && \
docker exec -it magiya_mysql_v2 mysql -uroot -ppassword magiya_v2 -e "SHOW TABLES;" && \
docker compose exec migrator python main.py