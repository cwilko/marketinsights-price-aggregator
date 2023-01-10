FROM arm32v7/python:3.7-slim-buster AS compile-image

COPY qemu-arm-static /usr/bin
COPY jobs/requirements.txt .

RUN apt-get -y update && \
    apt-get -y install --no-install-recommends llvm-11 llvm-11-dev build-essential git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install wheel 
RUN LLVM_CONFIG=llvm-config-11 pip install llvmlite==0.39.1
RUN pip install --extra-index-url https://www.piwheels.org/simple -r requirements.txt

FROM arm32v7/python:3.7-slim-buster AS build-image

COPY qemu-arm-static /usr/bin
COPY --from=compile-image /opt/venv /opt/venv

RUN apt-get -y update && \
    apt-get -y install --no-install-recommends libatlas3-base llvm-11-runtime && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1
ENV PATH="/opt/venv/bin:$PATH"

RUN mkdir -p /usr/app
COPY . /usr/app
WORKDIR /usr/app/jobs

CMD ["python", "-u", "app.py"]