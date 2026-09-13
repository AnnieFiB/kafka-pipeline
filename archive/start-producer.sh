#!/bin/bash

# Stock Market Data Producer Startup Script

echo "🚀 Starting Stock Market Data Pipeline..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file..."
    cat > .env << EOF
RAPIDAPI_KEY=your_rapidapi_key_here
KAFKA_TOPIC=stock_intraday_msft
EOF
    echo "⚠️  Please update .env file with your RapidAPI key!"
fi

# Start the services
echo "🐳 Starting Docker services..."
docker-compose up -d kafka postgres

echo "⏳ Waiting for Kafka to be ready..."
sleep 30

echo "📊 Starting stock data producer..."
docker-compose up -d stock-producer

echo "🌐 Starting API service..."
docker-compose up -d api

echo "✅ All services started!"
echo ""
echo "📋 Service URLs:"
echo "   API Documentation: http://localhost:8000"
echo "   API Health Check: http://localhost:8000/health"
echo "   Kafka UI: http://localhost:8082"
echo "   PgAdmin: http://localhost:5050"
echo ""
echo "📊 To view logs:"
echo "   Producer logs: docker-compose logs -f stock-producer"
echo "   API logs: docker-compose logs -f api"
echo "   All logs: docker-compose logs -f"
echo ""
echo "🧪 Test the API:"
echo "   curl http://localhost:8000/status"
echo "   curl -X POST http://localhost:8000/collect-data"
echo ""
echo "🛑 To stop:"
echo "   docker-compose down"
