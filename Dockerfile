# ---------- Base Python ----------
FROM python:3.11-slim

# ---------- Sistema / Chromium ----------
RUN apt-get update && apt-get install -y --no-install-recommends \
      chromium chromium-driver \
      && rm -rf /var/lib/apt/lists/*

# ---------- Variáveis que o Selenium usará ----------
# (o código vai ler os paths por padrão, mas já deixamos claro)
ENV CHROMIUM_BIN=/usr/bin/chromium \
    CHROMEDRIVER_BIN=/usr/bin/chromedriver \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# ---------- Diretório de trabalho ----------
WORKDIR /app
COPY . /app

# ---------- Dependências Python ----------
RUN pip install --no-cache-dir -r requirements.txt

# ---------- Expõe porta interna ----------
EXPOSE 8000

# ---------- Comando de inicialização ----------
CMD ["uvicorn", "booking_full_api:app", "--host", "0.0.0.0", "--port", "8000"]
