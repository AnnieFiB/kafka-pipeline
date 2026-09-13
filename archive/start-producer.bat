@echo off
REM Stock Market Data Producer Startup Script for Windows

echo 🚀 Starting Stock Market Data Pipeline...

REM Check if Docker is running
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Docker is not running. Please start Docker first.
    pause
    exit /b 1
)

REM Create .env file if it doesn't exist
if not exist .env (
    echo 📝 Creating .env file...
    (
        echo RAPIDAPI_KEY=your_rapidapi_key_here
        echo KAFKA_TOPIC=stock_intraday_msft
    ) > .env
    echo ⚠️  Please update .env file with your RapidAPI key!
)

REM Start the services
echo 🐳 Starting Docker services...
docker-compose up -d kafka postgres

echo ⏳ Waiting for Kafka to be ready...
timeout /t 30 /nobreak >nul

echo 📊 Starting stock data producer...
docker-compose up -d stock-producer

echo 🌐 Starting API service...
docker-compose up -d api

echo ✅ All services started!
echo.
echo 📋 Service URLs:
echo    API Documentation: http://localhost:8000
echo    API Health Check: http://localhost:8000/health
echo    Kafka UI: http://localhost:8082
echo    PgAdmin: http://localhost:5050
echo.
echo 📊 To view logs:
echo    Producer logs: docker-compose logs -f stock-producer
echo    API logs: docker-compose logs -f api
echo    All logs: docker-compose logs -f
echo.
echo 🧪 Test the API:
echo    curl http://localhost:8000/status
echo    curl -X POST http://localhost:8000/collect-data
echo.
echo 🛑 To stop:
echo    docker-compose down
echo.
pause
