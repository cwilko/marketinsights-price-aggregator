FROM arm64v8/python:3.8-slim-bullseye 

ENV PYTHONUNBUFFERED=1

RUN apt-get -y update && \
    apt-get -y install --no-install-recommends git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /usr/app
COPY . /usr/app
WORKDIR /usr/app

RUN pip install wheel
RUN pip install --extra-index-url https://www.piwheels.org/simple -r requirements.txt

CMD ["python", "-u", "./jobs/app.py"]