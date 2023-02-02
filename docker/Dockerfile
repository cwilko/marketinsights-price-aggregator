FROM arm64v8/python:3.9-slim-bullseye as build-image

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .

RUN apt-get -y update && \
    apt-get -y install --no-install-recommends git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install wheel
RUN pip install -r requirements.txt

FROM build-image

COPY --from=build-image /opt/venv /opt/venv

ENV PYTHONUNBUFFERED=1
ENV PATH="/opt/venv/bin:$PATH"

RUN mkdir -p /usr/app
COPY . /usr/app
WORKDIR /usr/app/jobs

CMD ["python", "-u", "app.py"]