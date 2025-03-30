# # Use official Python image
# FROM python:3.11

# WORKDIR /app

# COPY . .

# RUN apt-get update && apt-get install -y default-mysql-client

# RUN pip install --no-cache-dir pyspark pandas sqlalchemy pymysql seaborn matplotlib python-dotenv

# # ✅ Copy the .env file into the container
# COPY .env .env

# CMD ["tail", "-f", "/dev/null"]


# Use official Python image
FROM python:3.9

# Set working directory
WORKDIR /app

# Copy files into the container
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables from .env file
ENV MYSQL_HOST=mysql
ENV MYSQL_USER=root
ENV MYSQL_PASSWORD=dragonemperor
ENV MYSQL_DATABASE=retail_db

# Run the script
CMD ["python", "scripts/load.py"]
