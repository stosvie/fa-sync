# For more information, please refer to https://aka.ms/vscode-docker-python


#FROM python:3.8-slim-buster
FROM python:3.6

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

EXPOSE 5000

# See about installing ODBC drivers here: https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-2017
# Note that the driver version installed needs to match the version used in the code
# In this case for Ubuntu 18.04: ODBC SQL driver 17
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update -y
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev mssql-tools



# Install pip requirements
COPY requirements.txt .
RUN python -m pip install -r requirements.txt


WORKDIR /app
COPY . /app

# Switching to a non-root user, please refer to https://aka.ms/vscode-docker-python-user-rights
RUN useradd -m appuser && chown -R appuser /app 
#COPY  ./.flickr /home/appuser
#COPY ./.flickr app
USER appuser

# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
# CMD ["python", "run.py"]

# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
CMD ["gunicorn", "--timeout", "1000", "--bind",  "0.0.0.0:5000", "app:app"]
