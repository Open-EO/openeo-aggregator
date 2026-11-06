# Builder stage: build the openeo-aggregator wheel from the source tree of the build context
FROM python:3.11-slim-bookworm AS builder

WORKDIR /build

RUN python -m pip install --upgrade pip
RUN python -m pip install --upgrade build
COPY . /build
RUN python -m build --wheel --outdir /build/wheels


# Runtime stage
FROM python:3.11-slim-bookworm

# Install OS updates (https://pythonspeed.com/articles/docker-cache-insecure-images/)
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install libexpat1 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


# Set some global pip defaults.
ENV PIP_CONF="/etc/pip.conf"
RUN echo "[global]" >> $PIP_CONF && \
    # Disable pip download cache to reduce image size (https://pythonspeed.com/articles/smaller-docker-images/)
    # Note the counter-intuitive double negation handling of this config (also see https://github.com/pypa/pip/issues/5011)
    echo "no-cache-dir = false" >> $PIP_CONF && \
    echo "timeout = 60" >> $PIP_CONF && \
    cat $PIP_CONF


# Build and run as non-root (see https://pythonspeed.com/articles/root-capabilities-docker-security/)
RUN groupadd --gid 1000 openeo && \
    useradd --create-home --base-dir /home --uid 1000 --gid openeo openeo

WORKDIR /home/openeo
USER openeo

# Set up virtualenv (see https://pythonspeed.com/articles/activate-virtualenv-dockerfile/)
ENV VIRTUAL_ENV="/home/openeo/venv"
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN python -m pip install --upgrade pip


# Use ARG trick with to allow triggering a docker cache miss at this point.
# Depends `--build-arg` usage, e.g. as done from jenkinslib
ARG CACHE_DATE=0 PACKAGE_NAME=0
RUN echo $CACHE_DATE $PACKAGE_NAME


# First install locked third-party dependencies (better layer caching),
COPY --chown=openeo docker/requirements.txt .
RUN python -m pip install -r requirements.txt

# Then install the openeo-aggregator wheel built in the builder stage
# (mounted, so it doesn't end up in a layer).
RUN --mount=from=builder,source=/build/wheels,target=/wheels \
    python -m pip install /wheels/openeo_aggregator-*.whl

# Fail the build if the lockfile is out of sync with the package dependencies
RUN python -m pip check

CMD ["gunicorn", "openeo_aggregator.app:create_app()"]
