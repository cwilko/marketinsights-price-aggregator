FROM arm32v7/python:3.8-slim-buster

COPY qemu-arm-static /usr/bin

ENV PYTHONUNBUFFERED=1

RUN mkdir -p /usr/app
COPY nightly /usr/app
WORKDIR /usr/app


RUN apt-get -y update && \
    apt-get -y install --no-install-recommends git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --extra-index-url https://www.piwheels.org/simple -r requirements.txt

CMD ["python", "-u", "app.py"]