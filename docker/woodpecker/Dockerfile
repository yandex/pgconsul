FROM pgconsulbase:latest
RUN pip3 install psycopg2-binary
COPY woodpecker.py /usr/local/bin/woodpecker.py
RUN chmod +x /usr/local/bin/woodpecker.py
CMD ["python3", "/usr/local/bin/woodpecker.py"]
