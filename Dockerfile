FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        mdbtools \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /repo

COPY requirements-lock.txt requirements-lock.txt
RUN python -m pip install --upgrade pip \
    && python -m pip install -r requirements-lock.txt

COPY . .

CMD ["bash", "Scripts/QA_QC/release_gate.sh"]
