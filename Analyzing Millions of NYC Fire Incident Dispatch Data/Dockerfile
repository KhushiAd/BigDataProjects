# We want to go from the base image of python:3.9
FROM python:3.9

# This is the equivalent of “cd /app” from our host machine
WORKDIR /app

# Let’s copy everything into /app
COPY .  /app

# Installs the dependencies. Passes in a text file.
RUN pip install -r requirements.txt

# This will run when we run our docker container
ENTRYPOINT ["python", "main.py"]