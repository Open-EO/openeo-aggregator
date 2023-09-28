FROM python:3.8-slim-buster


# Workaround for IPv4/IPv6 networking performance issues.
# (https://stackoverflow.com/questions/65760510/readtimeouterror-pip-not-installling-any-library)
RUN echo "precedence ::ffff:0:0/96  100" >> /etc/gai.conf


# Install OS updates (https://pythonspeed.com/articles/docker-cache-insecure-images/)
RUN apt-get update && \
    apt-get upgrade -y && \
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


# Copy source code
RUN mkdir /home/openeo/aggregator
WORKDIR /home/openeo/aggregator
COPY --chown=openeo setup.py setup.py
COPY --chown=openeo src src
COPY --chown=openeo conf conf
COPY --chown=openeo pytest.ini pytest.ini
COPY --chown=openeo tests tests
COPY --chown=openeo CHANGELOG.md CHANGELOG.md


# Install dependencies and app.
RUN pip install --upgrade pip && \
    pip install .


CMD ["gunicorn", "--config=conf/gunicorn.prod.py", "openeo_aggregator.app:create_app()"]
