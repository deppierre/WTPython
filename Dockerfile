#docker build -t wiredtiger .
#docker run --name wiredtiger -v /Users/pierre.depretz/Documents/2_git/WTPython/data/db:/data wiredtiger ./wt_dump.py -m mydb.mycollection /data

FROM python:3.9

ENV CMAKE_LIBRARY_PATH=/usr/lib/aarch64-linux-gnu

RUN apt-get update &&\
    apt-get -y install libsnappy-dev zlib1g libzstd-dev python3-dev swig

RUN python -m ensurepip --upgrade &&\
    python -m pip install wiredtiger regex pymongo

WORKDIR /wiredtiger

COPY wt_dump.py .
COPY ksdecode .

RUN chmod +x wt_dump.py