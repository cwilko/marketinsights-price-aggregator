FROM arm64v8/python:3.9-slim-bullseye 

ENV PYTHONUNBUFFERED=1

RUN mkdir -p /usr/app
COPY . /usr/app
WORKDIR /usr/app

RUN pip install -r requirements.txt

CMD ["python", "-u", "./jobs/app.py"]