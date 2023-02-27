ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=11
ARG PYTHON_VERSION=3.9.8

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}
RUN  apt-get update \
  && apt-get install -y wget \
  && rm -rf /var/lib/apt/lists/*

COPY --from=py3 / /

ARG PYSPARK_VERSION=3.3.2
RUN pip --no-cache-dir install pyspark==${PYSPARK_VERSION} notebook

ARG USERNAME=user
ARG USER_UID=1000
ARG USER_GID=$USER_UID

# Create the user
RUN useradd -m $USERNAME  

USER $USERNAME

WORKDIR $HOME
