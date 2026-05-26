FROM apache/spark:3.5.1-python3

USER root

WORKDIR /opt/spark/work-dir/app

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN mkdir -p /opt/spark/work-dir/app/logs \
    /opt/spark/work-dir/app/data/raw \
    /opt/spark/work-dir/app/data/bronze \
    /opt/spark/work-dir/app/data/silver \
    /opt/spark/work-dir/app/data/gold \
    /opt/spark/work-dir/app/data/powerbi \
    /opt/spark/work-dir/app/data/checkpoints

RUN chmod -R 777 /opt/spark/work-dir/app

USER root