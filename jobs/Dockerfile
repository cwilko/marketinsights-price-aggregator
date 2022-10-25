FROM arm32v7/python:3.9-slim-bullseye AS compile_image

COPY qemu-arm-static /usr/bin
COPY nightly/requirements.txt .

RUN apt-get -y update && \
    apt-get -y install --no-install-recommends libatlas3-base llvm-9 llvm-9-dev build-essential git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN LLVM_CONFIG=llvm-config-9 pip install llvmlite==0.33
RUN pip install --extra-index-url https://www.piwheels.org/simple -r requirements.txt

FROM arm32v7/python:3.9-slim-bullseye AS build_image

COPY qemu-arm-static /usr/bin
COPY --from=compile-image /opt/venv /opt/venv

ENV PYTHONUNBUFFERED=1
ENV PATH="/opt/venv/bin:$PATH"

RUN mkdir -p /usr/app
COPY . /usr/app
WORKDIR /usr/app/nightly

CMD ["python", "-u", "app.py"]