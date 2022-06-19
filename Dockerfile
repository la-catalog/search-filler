FROM python:3.10.1-slim-buster

RUN apt update
RUN apt upgrade -y

WORKDIR /app

COPY requirements.txt /app
RUN pip install -r requirements.txt

COPY . /app

ENTRYPOINT ["python", "app/run.py"]