# wowauc.info data fetch node
FROM debian:jessie
MAINTAINER Lev "Gourytch" Babchenko <gour@mail.ru>

WORKDIR /data
VOLUME /data

RUN apt-get update && apt-get -y dist-upgrade
RUN apt-get install -y \
    python python-pycurl

CMD ["/bin/bash", "/data/wowauc_mdb/wowauc_fetch.sh"]
