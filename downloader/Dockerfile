FROM python:3.10-alpine as base
WORKDIR /app
COPY requirements.txt .

RUN apk add --upgrade \
    gcc \
    musl-dev
RUN pip install --user -r requirements.txt

FROM python:3.10-alpine
COPY --from=base /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH

WORKDIR /app
COPY . .
ENTRYPOINT [ "python", "main.py" ]