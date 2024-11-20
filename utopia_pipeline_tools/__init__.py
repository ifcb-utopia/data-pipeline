"""
utopia_pipeline_tools: a set of modules to streamline the process of converting
raw IFCB data to CNN-classified and validated datasets. 
"""

# a bit of metadata
__version__ = '0.2.7'
__author__ = 'Claire Berschauer'
__credits__ = 'Applied Physics Laboratory - UW'

# global attributes
label_dict = {'Chloro': 0,
              'Cilliate': 1,
              'Crypto': 2,
              'Diatom': 3,
              'Dictyo': 4,
              'Dinoflagellate': 5,
              'Eugleno': 6,
              'Other': 7,
              'Prymnesio': 8,
              'Null': 9
             }

label_list = list(label_dict.keys())

config_info = {"blob_storage_name": 'ifcbwestus',
               "connection_string": '''DefaultEndpointsProtocol=https;AccountName=ifcbwestus;AccountKey=bHLeaeVSCT4DK7nwzof/lTmKctp6+ljI9e95efaZTM8klc6MwMseApKN9qRT8GXFxwLnroDtvXGE+ASt14eASA==;EndpointSuffix=core.windows.net''',
               "server": 'ifcbserver.database.windows.net',
               "database": 'ifcbsql',
               "db_user": 'ifcbadmin',
               "db_password": 'Phyt0plankton!',
               'subscription_id': '91804dbe-1fd2-4384-8b66-2b5e4ad1f2f2',
               'resource_group': 'UTOPIA',
               'workspace_name': 'PIVOT',
               'experiment_name': 'adt-pivot',
               'api_key': '4B2DTMyhNHgIk3lJtt8MSdBU5QodpCNf',
               'model_name': 'basemodel',
               'endpoint_name': 'basemodel-endpoint',
               'deployment_name': 'pivot-basemodel',
               }

default_investigators = {"Ali_Chase": ['APL-UW', 'alichase@uw.edu'],
                         "Claire_Berschauer": ['APL-UW', 'ckb22@uw.edu'],
                         }