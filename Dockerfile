FROM python:3.9-slim-buster

ENV PIP_NO_CACHE_DIR=on \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_VERSION=1.2.2


# Install Java things so that Spark works.
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/

RUN  apt-get update -y \
    && apt-get install -y software-properties-common \
    && add-apt-repository ppa:openjdk-r/ppa \
    && apt-get install -y openjdk-11-jdk \
    && apt-get install -y procps \
    && export JAVA_HOME \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN python -m pip install poetry==$POETRY_VERSION

COPY pyproject.toml poetry.lock ./
RUN poetry export --without-hashes -f requirements.txt -o requirements.txt && \
    python -m pip install -r requirements.txt

COPY src ./src
COPY kaggle.json /root/.kaggle/kaggle.json

RUN poetry install --no-interaction --no-ansi --only-root
