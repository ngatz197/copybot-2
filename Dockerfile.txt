FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot code
COPY . .

# Expose the health check port
ENV PORT=8080

# Run the bot
CMD ["python", "bot.py"]
