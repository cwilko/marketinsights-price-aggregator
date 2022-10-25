FROM arm32v7/python:3.9-slim-bullseye

COPY qemu-arm-static /usr/bin

ENV PYTHONUNBUFFERED=1

RUN mkdir -p /usr/app
COPY . /usr/app
WORKDIR /usr/app/nightly


RUN apt-get -y update && \
    apt-get -y install --no-install-recommends libatlas3-base llvm-9 git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN LLVM_CONFIG=llvm-config-9 \
    pip install --extra-index-url https://www.piwheels.org/simple llvmlite=0.33

RUN pip install --extra-index-url https://www.piwheels.org/simple -r requirements.txt

CMD ["python", "-u", "app.py"]