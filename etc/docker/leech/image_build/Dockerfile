# wowauc.info data fetch node
FROM debian:jessie
MAINTAINER Lev "Gourytch" Babchenko <gour@mail.ru>

RUN apt-get update && apt-get -y dist-upgrade
RUN apt-get install -y \
    python python-pycurl

RUN useradd \
  --uid 1000 --gid 100 --no-user-group \
  --home /data --shell /bin/bash leech
RUN mkdir /data && chmod 0777 /data && chown -R leech:users /data

USER leech
ENV HOME /data
WORKDIR /data
VOLUME /data
CMD ["/bin/bash", "/data/wowauc_mdb/wowauc_fetch.sh"]
