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