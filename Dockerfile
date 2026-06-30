FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN echo "options single-request" >> /etc/resolv.conf
RUN bash -c '</dev/tcp/1.1.1.1/443' && echo "✅ Интернет есть" || echo "❌ Интернета нет (Security Groups блокируют исходящий трафик)"
#RUN ip a
# 2. Устанавливаем curl, принудительно используя IPv4
RUN apt-get update -o Acquire::ForceIPv4=true && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY settings ./settings

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
