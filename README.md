# nextgeoss-scripts
Scripts used for automating operations on NextGeoss (creating datasets, etc).


## How to run the script
1. Create a vritual env (Note: the script still uses python2)

  ```  virtualenv -p /path/to/python2 venv ```
  
2. Install the dependencies

  ``` pip install -r requirements.txt ```
  

3. Set up the information in the `credentials.ini` file
```
ckan_url = url to the ckan portal
ckan_api_token = sys admin API key
```

4. Run the script and follow the questions

``` python output_datasets.py ```


## How to get the credentials for the credentials.ini file

In order to be able to run the script, you need to add the credentials to the credentials.ini file.

The `ckan_url` parameter is the url to your ckan portal.

Example: `ckan_url = http://127.0.0.1`

The `ckan_api_token` is the API key. The user whos API key we are going to use for running the script, must have permissions to add new Topics to the ckan portal.

In order to get the API KEY, you need to login to your CKAN portal, and go to the users page `ckan_url/user/user_name`. You can also do this by clicking on your user name in the top right of the navigation.

Example: `ckan_api_token = QWERTY`
