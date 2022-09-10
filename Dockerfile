FROM python:3.8
COPY requirements.txt /prediccion/requirements.txt
WORKDIR /prediccion
RUN pip install -r requirements.txt
COPY . /prediccion
VOLUME /tmp
ENTRYPOINT ["python"]
CMD ["main.py"]