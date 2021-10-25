FROM python:3.9-alpine
FROM amancevice/pandas:1.1.2-alpine

RUN apk add --update --no-cache g++ gcc libxslt-dev jpeg-dev && \
    pip3 install --upgrade pip

RUN mkdir -p /root/DEMO-arXiv 

WORKDIR /root/DEMO-arXiv

COPY . .

RUN pip3 install -r requirements.txt 

CMD python import.py
