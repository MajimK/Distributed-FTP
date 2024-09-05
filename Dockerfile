FROM python:3.11-slim

WORKDIR /app

COPY application/ /app/application/
COPY communication/ /app/communication/
COPY data_access/ /app/data_access/
COPY database/ /app/database/
COPY dht/ /app/dht/
COPY utils/ /app/utils/
COPY main.py /app/main.py

EXPOSE 21 20

CMD ["python3", "-u", "main.py"]
