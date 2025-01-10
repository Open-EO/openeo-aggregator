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
    echo "extra-index-url = https://artifactory.vgt.vito.be/api/pypi/python-openeo/simple" >> $PIP_CONF && \
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
RUN pip install --upgrade pip


# Use ARG trick with to allow triggering a docker cache miss at this point.
# Depends `--build-arg` usage, e.g. as done from jenkinslib
ARG CACHE_DATE=0 PACKAGE_NAME=0


# Install openeo-aggregator
COPY --chown=openeo docker/requirements.txt .
RUN pip install -r requirements.txt

CMD ["gunicorn", "openeo_aggregator.app:create_app()"]
