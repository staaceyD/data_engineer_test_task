# Data Engineer Test Task
The application that extracts JSON files from the public AWS S3 bucket, transform them, and load results to the database.

## Instalation
To launch this project first you need to clone the repository.

```bash
git clone 'repository link'
```

Сreate virtual environment and activate it

```bash
python3 -m venv <name of virtual env folder>
source <name of virtual env folder>/bin/activate
```
After thet you can install all dependencies

```bash
pip3 install -r requirements.txt 
```

Install aws CLI and cofigure user for further usage:

```bash
pip3 install awscli --upgrade --user
aws configure
```

Here you should be good to go. To start the project run db_wrapper.py file
```bash
python3 db_wrapper.py
```

This will process files in the bucket, create database and insert records to the database