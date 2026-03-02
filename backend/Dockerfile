# syntax=docker/dockerfile:1.6
ARG UBUNTU_VERSION=22.04
FROM ubuntu:${UBUNTU_VERSION}

ENV DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    TZ=Etc/UTC \
    PYTHONUNBUFFERED=1 \
    MPLCONFIGDIR=/tmp/matplotlib

# ---- system deps (stable) ----
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    software-properties-common \
    wget \
    gnupg \
    build-essential \
    git \
    python3 \
    python3-venv \
    python3-pip \
    python3-dev \
    python3-distutils \
    r-base \
    gfortran \
    libatlas-base-dev \
    libfreetype6-dev \
    libxft-dev \
    libxext6 \
    libxrender1 \
    libsm6 \
    libxt6 \
    libffi-dev \
    libssl-dev \
    libxml2-dev \
    libcurl4-openssl-dev \
    libbz2-dev \
    liblzma-dev \
    zlib1g-dev \
    fonts-dejavu-core \
    libreadline-dev \
    libncursesw5-dev \
    libsqlite3-dev \
    libgdbm-dev \
    libgdbm-compat-dev \
    libnss3 \
    libexpat1-dev \
    tk-dev \
    uuid-dev \
    xz-utils \
 && rm -rf /var/lib/apt/lists/*

# ---- Python 3.6 build (stable, expensive) ----
ARG PY36_VERSION=3.6.15
ARG PY36_PREFIX=/opt/python3.6
RUN curl -fsSL https://www.python.org/ftp/python/${PY36_VERSION}/Python-${PY36_VERSION}.tgz -o /tmp/Python-${PY36_VERSION}.tgz \
 && tar -xzf /tmp/Python-${PY36_VERSION}.tgz -C /tmp \
 && cd /tmp/Python-${PY36_VERSION} \
 && ./configure --prefix=${PY36_PREFIX} --with-ensurepip=install \
 && make -j"$(nproc)" \
 && make install \
 && rm -rf /tmp/Python-${PY36_VERSION} /tmp/Python-${PY36_VERSION}.tgz

# ---- main venv ----
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv ${VIRTUAL_ENV}
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

# Copy requirements FIRST for caching
COPY backend/requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip \
 && pip install --no-cache-dir -r /tmp/requirements.txt \
 && rm /tmp/requirements.txt

# ---- anomaly env (py3.6) ----
ENV AD_PYTHON_ENV=/opt/py36
RUN ${PY36_PREFIX}/bin/python3.6 -m venv ${AD_PYTHON_ENV} \
 && ${AD_PYTHON_ENV}/bin/pip install --upgrade pip==21.3.1

COPY backend/requirements-ad.txt /tmp/requirements-ad.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    ${AD_PYTHON_ENV}/bin/pip install --no-cache-dir -r /tmp/requirements-ad.txt \
 && rm /tmp/requirements-ad.txt

# ---- R deps (stable-ish) ----
RUN Rscript -e "install.packages(c('optparse','readr','dplyr','lubridate','stringr','ggplot2','patchwork','remotes'), repos='https://cloud.r-project.org')" \
 && Rscript -e "remotes::install_github('humanfactors/FIPS', upgrade = 'never', dependencies = TRUE, build_vignettes = FALSE)"

RUN mkdir -p /data

# ---- app code (changes often) ----
WORKDIR /app

COPY backend/ /app/

ENV UNIFIED_DATA_ROOT=/data \
    AD_DETECTORS_PYTHON=${AD_PYTHON_ENV}/bin/python \
    MATLAB_SERVICE_URL=http://matlab:9000

RUN chmod +x /app/entrypoint.sh \
 && ln -s /app/entrypoint.sh /usr/local/bin/unified-entrypoint

ENTRYPOINT ["unified-entrypoint"]
CMD ["python", "/app/app.py"]