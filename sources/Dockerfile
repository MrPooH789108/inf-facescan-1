FROM alpine

RUN apk add --no-cache tzdata
ENV TZ=Asia/Bangkok

ENV PYTHONUNBUFFERED=1
RUN apk add --update --no-cache python3 && ln -sf python3 /usr/bin/python
RUN python3 -m ensurepip
RUN pip3 install --no-cache --upgrade pip setuptools
RUN apk update && apk add python3-dev \
                        gcc \
                        libc-dev

#Install requirement
COPY requirements.txt requirements.txt
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt

# copy source code and create log dir
COPY src/ .
RUN mkdir -p /var/log/innoflex/

#run code
COPY env_secrets_expand.sh /env_secrets_expand.sh
COPY infapp.sh /infapp.sh
RUN chmod u+x infapp.sh
ENTRYPOINT "/infapp.sh"



