import requests
import json
import pandas as pd
from pandas import json_normalize
import time
import pyodbc
import sys
from loguru import logger as Log 

headers = {'Content-Type': 'application/x-www-form-urlencoded',
    'Ocp-Apim-Subscription-Key': 'SUB_KEY'}

x = 'API_URL'
address = x + "authorize/token?"

body = {'grant_type': 'client_credentials',
        'client_id':'CLIENT_ID',
        'client_secret':'SECRET',
        'scope':'SCOPE'}


response = requests.post(address, headers=headers,data=body)

response.raise_for_status()

data = response.json()
token = data['access_token']

#### ----- Loops pages in url ----- #####
pages = range(1)

ids = []
for page in pages:

    page = str(page)
    

    url = 'URL'
    bearer = 'Bearer ' + token
    headers = {
        'Tenant':'TENANT',
        'authorization': bearer,
        'Ocp-Apim-Subscription-Key': 'SUB KEY',     #Primary
        'content-type': "application/json"

        }


    r = requests.request("GET", url, headers=headers)

    #r.raise_for_status() #If there is an error, code stops here and displays error
    lists = r.json()
    ids.extend(lists)
  
# ##### ----- Will pull company IDs from lists and loop the IDs in cUrl -----#####
   
    i = []
    ind = []
    append_data = []

    for company in ids:

        sCompany = str(company)    #converts int(s) to string for URL
        url =  'URL'     #forms API call and adds company# from list
        request = requests.request("GET", url, headers=headers)     #Calls API to get data
        time.sleep(1)     #in seconds
        jCompany = request.json()     #converts data to json
          
    #####----- SQL Connection -----#####    

        server = 'SERVER NAME'
        database = 'DATABASE NAME'


        conn =  pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server='+ server+ ';Database=' + database + ';Trusted_Connection=yes')
        #conn =  pyodbc.connect('Driver={SQL Server Native Client 11.0};Server='+ server+ ';Database=' + database + ';Trusted_Connection=yes')
        cursor = conn.cursor()

        cursor.execute('TRUNCATE TABLE dbo.companiesID')
        cursor.commit()
        try:
            for row in jCompany:
                cursor.execute('''
                       
                        INSERT INTO companiesID (CompaniesId,Name,BillingAddress,FrontOfficeId,Link,IsArchived,RepresentativeUsers,StatusId,Status,Industry,CreatedDate,\
                        LastUpdatedDate,LastActivityDate,LatestActivityName,OpenJobs,Street1,Street2,City,StateOrProvince,PostalCode,Country,County,GeoCode,SchoolDistrictCode) 
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ''',
                        (
                        jCompany['id'                                ],
                        jCompany['name'                              ],
                        jCompany['billingAddress'                    ],
                        jCompany['frontOfficeId'                     ],
                        jCompany['link'                              ],
                        jCompany['isArchived'                        ],
                        jCompany['representativeUsers'               ],
                        jCompany['statusId'                          ],
                        jCompany['status'                            ],
                        jCompany['industry'                          ],
                        jCompany['createdDate'                       ],
                        jCompany['lastUpdatedDate'                   ],
                        jCompany['latestActivityDate'                ],
                        jCompany['latestActivityName'                ],
                        jCompany['openJobs'                          ],
                        jCompany['mainAddress']['street1'            ],
                        jCompany['mainAddress']['street2'            ],
                        jCompany['mainAddress']['city'               ],
                        jCompany['mainAddress']['state_Province'     ],
                        jCompany['mainAddress']['postalCode'         ],
                        jCompany['mainAddress']['country'            ],
                        jCompany['mainAddress']['county'             ],
                        jCompany['mainAddress']['geoCode'            ],
                        jCompany['mainAddress']['schoolDistrictCode' ]
                        )
                    )

                cursor.commit()
        except Exception as e:
            print('Exception occurred', e.args[0]) 


