FROM python:3.10.4-slim-bullseye

WORKDIR /usr/src/app

COPY requirements.txt ./

RUN apt update -y && apt install --no-install-recommends -y sqlite3 && \
    pip install --no-cache-dir -r requirements.txt && \

ENV LC_ALL=C
ENV DISCORDID=PLACEHOLDER
ENV DISCORDTOKEN=PLACEHOLDER

COPY . .

CMD [ "python", "./main.py" ]