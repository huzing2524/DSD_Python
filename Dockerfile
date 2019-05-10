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

RUN mkdir -p /app

COPY . /app

WORKDIR /app

RUN pip install -r requirements.txt

EXPOSE 8001

CMD ["python3", "./department/manage.py", "runserver", "0.0.0.0:8001"]
