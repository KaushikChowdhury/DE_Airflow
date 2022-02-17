FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /home/app

COPY upload_data.py upload_data.py

# EXPOSE 5432

ENTRYPOINT [ "python", "upload_data.py" ]