FROM python:3.14-slim

# Microsoft ODBC Driver 18 for SQL Server
RUN . /etc/os-release     && apt-get update     && apt-get install -y --no-install-recommends curl ca-certificates     && curl -fsSLo /tmp/packages-microsoft-prod.deb "https://packages.microsoft.com/config/debian/${VERSION_ID}/packages-microsoft-prod.deb"     && dpkg -i /tmp/packages-microsoft-prod.deb     && apt-get update     && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 libgssapi-krb5-2    && rm /tmp/packages-microsoft-prod.deb     && apt-get purge -y --auto-remove curl     && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY indexmanager indexmanager

RUN useradd --system --uid 10001 --no-create-home app \
    && mkdir data .auth \
    && chown app:app data .auth

USER app
VOLUME /app/data

ENV APP_HOST=0.0.0.0 \
    PYTHONUNBUFFERED=1

EXPOSE 8765

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8765/login', timeout=3)"]

CMD ["python", "app.py"]
