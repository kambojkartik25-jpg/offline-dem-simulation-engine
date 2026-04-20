FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN set -e; \
		pip install --no-cache-dir \
			--trusted-host pypi.org \
			--trusted-host files.pythonhosted.org \
			-r /app/requirements.txt

COPY pyproject.toml /app/pyproject.toml
COPY src /app/src

RUN set -e; \
		pip install --no-cache-dir \
			--trusted-host pypi.org \
			--trusted-host files.pythonhosted.org \
			-e .

EXPOSE 8000

CMD ["python", "-m", "dem_sim.web", "--host", "0.0.0.0", "--port", "8000"]

