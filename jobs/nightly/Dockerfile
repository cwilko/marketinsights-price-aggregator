FROM arm32v7/python:3.7-slim-buster

COPY qemu-arm-static /usr/bin

ENV PYTHONUNBUFFERED=1
ENV HDF5_USE_FILE_LOCKING=FALSE

RUN mkdir -p /usr/app
COPY . /usr/app
WORKDIR /usr/app


RUN apt-get -y update && \
    apt-get -y install --no-install-recommends libatlas3-base libhdf5-103 git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --extra-index-url https://www.piwheels.org/simple -r requirements.txt

CMD ["python", "-u", "app.py"]