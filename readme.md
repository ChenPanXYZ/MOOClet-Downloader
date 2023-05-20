
You need to have Python 3.9 installed. You can install the Python in the way you prefer, or you can check out miniconda to create Python environments. https://docs.conda.io/en/latest/miniconda.html

Run the following to install required packages:
```
pip install -r requirements.txt
```
You need to make a .env file from .env-sample, and fill in the following information:
```
PSQL_HOST=
PSQL_PASSWORD=
PSQL_DATABASE=
PSQL_USER=
PSQL_PORT=
```
And write the mooclets you want to download data from in `list_of_mooclet_names.txt`
```
mooclet_A
mooclet_B
```
Run `python3 datadownloader.py` to start downloading the datasets. The downloaded datasets will be in `datasets` folder. The filename is the mooclet name.