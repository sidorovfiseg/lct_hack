FROM python:3.9-slim

WORKDIR /opt/app-root/src/

COPY src/lct_app/requirements.txt /opt/app-root/src/lct_app/

RUN pip install --no-cache-dir -r /opt/app-root/src/lct_app/requirements.txt
RUN mkdir files

ENV PYTHONPATH /opt/app-root/src
ENV PYTHONUNBUFFERED=1

CMD ["gunicorn", "--config", "/opt/app-root/src/lct_app/api/config/gunicorn.conf.py"]