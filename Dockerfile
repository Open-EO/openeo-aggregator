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
    echo "timeout = 60" >> $PIP_CONF && \
    echo "extra-index-url = https://artifactory.vgt.vito.be/api/pypi/python-openeo/simple" >> $PIP_CONF && \
    cat $PIP_CONF


# Build and run as non-root (see https://pythonspeed.com/articles/root-capabilities-docker-security/)
RUN useradd --create-home openeo
WORKDIR /home/openeo
USER openeo

# Set up virtualenv (see https://pythonspeed.com/articles/activate-virtualenv-dockerfile/)
ENV VIRTUAL_ENV="/home/openeo/venv"
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"


# Copy source code
WORKDIR /home/openeo/aggregator
COPY setup.py setup.py
COPY src src
COPY pytest.ini pytest.ini
COPY tests tests


# GDD-1099 Jenkinslib: change whl versions incompatible with pip 20.3
RUN pip install -U pip==20.2.4

# Install dependencies and app.
RUN pip install .


CMD ["gunicorn", "--workers=4", "--threads=1", "--bind=0.0.0.0:8080", "openeo_aggregator.app:create_app()"]
