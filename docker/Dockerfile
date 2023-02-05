FROM arm64v8/python:3.9-alpine 

ENV PYTHONUNBUFFERED=1

RUN mkdir -p /usr/app
COPY . /usr/app
WORKDIR /usr/app

RUN pip install wheel
RUN pip install -r requirements.txt

WORKDIR /usr/app/jobs

CMD ["python", "-u", "app.py"]