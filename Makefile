.PHONY: up down restart logs test clean build

# Start all services
up:
	docker-compose up -d --build

# Stop all services
down:
	docker-compose down

# Restart services
restart:
	docker-compose restart

# View logs
logs:
	docker-compose logs -f

# Run tests
test:
	pytest tests/ -v

# Clean up (remove volumes)
clean:
	docker-compose down -v
	docker system prune -f

# Build images
build:
	docker-compose build

# Initialize Airflow (first time setup)
init:
	docker-compose up -d postgres
	sleep 5
	docker-compose run --rm airflow-webserver airflow db init
	docker-compose run --rm airflow-webserver airflow users create \
		--username admin \
		--password admin \
		--firstname Admin \
		--lastname User \
		--role Admin \
		--email admin@example.com

# Run dbt manually
dbt-run:
	docker exec -u airflow sports_airflow_scheduler /home/airflow/.local/bin/dbt run --target prod -C /opt/airflow/dbt

# Open Airflow UI
airflow-ui:
	open http://localhost:8080

# Open Streamlit UI
streamlit-ui:
	open http://localhost:8501
