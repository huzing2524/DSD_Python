FROM python:3.6

ENV PG_DATABASE="db_dsd" \
    PG_USER="dsdUser" \
    PG_PASSWORD="dsdUserPassword" \
    PG_HOST="postgres" \
    PG_PORT="5432" \
    RM_HOST="127.0.0.1" \
    RM_PORT="5432" \
    REDIS_HOST="127.0.0.1" \
#    REDIS_PORT="6379" \
    REDIS_DATABASE="0" \
    SETTING_NAME="prod"

RUN mkdir -p /dsdpy-app

RUN apt-get update && \
    apt-get install -y \
	supervisor &&\
	rm -rf /var/lib/apt/lists/*

COPY supervisor-app.conf /etc/supervisor/conf.d/

COPY . /dsdpy-app

WORKDIR /dsdpy-app

RUN pip3 install -r requirements.txt

EXPOSE 8001

CMD ["supervisord", "-n"]