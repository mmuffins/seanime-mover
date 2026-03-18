FROM python:3.12-slim

WORKDIR /seanime

RUN apt-get update \
    && apt-get install -y --no-install-recommends gosu \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 1000 seanime \
    && useradd --uid 1000 --gid 1000 --home-dir /seanime --shell /usr/sbin/nologin seanime \
    && mkdir -p /source /dest /config

COPY mover.py /seanime/mover.py
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["python", "/seanime/mover.py"]
