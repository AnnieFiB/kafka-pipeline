FROM python:3.10

# Install cron
RUN apt-get update && apt-get install -y cron

# Create app directory
WORKDIR /app

# Copy files
COPY producer.py requirements.txt crontab.txt entrypoint.sh ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Setup cron
RUN crontab crontab.txt
RUN chmod +x entrypoint.sh

# Entrypoint
CMD ["./entrypoint.sh"]