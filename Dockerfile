FROM tiangolo/uvicorn-gunicorn-fastapi:latest

RUN pip install numpy pandas

COPY app /app

WORKDIR /app

EXPOSE 80
