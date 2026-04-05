FROM python:3.13-slim

# set workdir
WORKDIR /app

# copy requirements dulu (biar caching bagus)
COPY requirements.txt .

# install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# copy app
COPY . .

# expose port
EXPOSE 8018

# run app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
