import marimo

__generated_with = "0.8.17"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    return mo,


@app.cell
def __(mo):
    mo.md(
        r"""
        # PIVOT Initial SQL Database Setup
        ---
        """
    )
    return


@app.cell
def __(mo):
    mo.md(r"""## Instructions""")
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ### Initial Ingestion Steps 

        Are you using PIVOT with a new Azure SQL database? Here is a list of what is required to set it up! This notebook uses a subsection of the North Atlantic Aerosols and Marine Ecosystems Study (NAAMES) dataset to perform initial ingestion, but if you don't want this data included in your database, simply change the filepath to your CNN-classified .csv of choice* below and select its corresponding blob container name in the dropdown menu.

        1. Modify the config file in the PIVOT folder to contain the location and credentials of your Azure SQL database, Azure blob storage, and model storage. Confirm in the config section below that the values are correct.
        2. Select whether you want to use the default dataset or your own then wait for the notebooks to finish running - it is preparing your data for streamlined ingestion. This may take several minutes. Check the cell output for any warnings or messages.
        3. Create the structure of the database by clicking the 'Build database' button, which runs the queries in the `create_db.sql` file included in the PIVOT repository and creates the stored procedures and user-defined variables with queries.
        4. Perform initial data ingestion by clicking the 'Insert data to Images table' button. This step may take over an hour depending on the amount of data so this is a good time for a snack break.
        5. Add your model to the Models table by clicking 'Initial Models insert'.
        6. Insert data into the Predictions table by clicking the 'Insert data to Predictions table' button. This should be much faster than ingestion into the Images table but may still take several minutes.
        7. Click the 'Initial Dissimilarity insert' to add the metrics formulas to the Dissimilarity table.
        8. Select which metrics methods you want to include with the checkboxes in the 'Metrics' section, then insert data into the Metrics table by clicking 'Insert data to Metrics table'.
        9. Consult the 'Final checks' section to ensure everything has worked as expected. 

        __Congratulations, your database is ready!__

        \* The dataset must have probability columns `0` through `9`, a `filepath` column containing values beginning with `ml/`, and a `pred_label` column containing integer values in the range [0, 9]. The file must also include an index column at position 0.

        ### Message Key  
        These messages are not visible in code view. 

        - `INFO` tells you information about the actions taken by the cell. No user action required.
        - `ACTION REQUIRED` indicates you haven't filled out a prerequisite input in the notebook. Most likely, your container or data file hasn't been selected yet.
        - `WARNING` means something has gone wrong unrelated to the data, typically with the SQL server. Check your database availability.
        - `ERROR` tells you when something has gone wrong with the data. Check that your dataset structure matches the requirements and if you've correctly mapped the probability values with the predicted labels.
        """
    )
    return


@app.cell(hide_code=True)
def __(display, mo):
    display(mo.md("### Checklist"))
    display(mo.md(r"""_Here is a checklist to keep track of the steps you've completed. Checking a box will disable the corresponding button._"""))
    data_check = mo.ui.checkbox(label='Dataset selection')
    build_check = mo.ui.checkbox(label='Build database')
    images_check = mo.ui.checkbox(label='Insert data to Images table')
    models_check = mo.ui.checkbox(label='Add models to Models table')
    predictions_check = mo.ui.checkbox(label='Insert data to Predictions table')
    dissim_check = mo.ui.checkbox(label='Add formulas to Dissimilarity table')
    metrics_check = mo.ui.checkbox(label='Add data to Metrics table')
    display(mo.md(f"""
    {data_check}  
    {build_check}  
    {images_check}  
    {models_check}  
    {predictions_check}    
    {dissim_check}  
    {metrics_check}  
    """))
    return (
        build_check,
        data_check,
        dissim_check,
        images_check,
        metrics_check,
        models_check,
        predictions_check,
    )


@app.cell
def __():
    # imports
    # INFO: You may get a streamlit runtime warning, this can be ignored. Run the cell again if you want it to disappear
    import numpy as np
    import pandas as pd
    import os

    import azure.storage.blob
    from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

    import sys
    sys.path.append('../PIVOT/PIVOT')  # point to the PIVOT directory

    import utils.data_utils as du
    import utils.insert_data as idu
    import utils.sql_utils as sq
    from utils import load_config

    from tqdm.auto import trange, tqdm
    import concurrent.futures

    from importlib import reload
    reload(du)
    reload(idu)

    from IPython.display import display
    from utopia_pipeline_tools.azure_blob_tools import list_containers_in_blob
    import utopia_pipeline_tools as upt
    return (
        BlobClient,
        BlobServiceClient,
        ContainerClient,
        azure,
        concurrent,
        display,
        du,
        idu,
        list_containers_in_blob,
        load_config,
        np,
        os,
        pd,
        reload,
        sq,
        sys,
        tqdm,
        trange,
        upt,
    )


@app.cell
def __(mo):
    mo.md(r"""---""")
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(r"""## Checking the config file""")
    return


@app.cell
def __(display, load_config, mo, upt):
    config_dict = load_config()
    display(mo.md(f"""**Azure Blob and SQL Configuration Settings:**  
    Blob connection string: {config_dict['connection_string']}  
    Server name: {config_dict['server']}  
    Database name: {config_dict['database']}  
    Database username: {config_dict['db_user']}  
    Subscription ID: {config_dict['subscription_id']}  
    Resource group: {config_dict['resource_group']}  

    **Model Storage Configuration Settings**  
    Workspace name: {config_dict['workspace_name']}  
    Experiment name: {config_dict['experiment_name']}  
    API key: {config_dict['api_key']}  
    Model name: {config_dict['model_name']}  
    Endpoint name: {config_dict['endpoint_name']}  
    Deployment name: {config_dict['deployment_name']}  
    """))
    label_dict = upt.label_dict
    label_list = upt.label_list
    return config_dict, label_dict, label_list


@app.cell
def __(mo):
    mo.md(r"""---""")
    return


@app.cell
def __(mo):
    mo.md(r"""## Dataset selection and preprocessing""")
    return


@app.cell
def __(mo):
    select_data = mo.ui.dropdown(['Default', 'Custom Dataset']).form()
    mo.md(f"Select default or custom dataset: {select_data}")
    return select_data,


@app.cell
def __(config_dict, display, list_containers_in_blob, mo, select_data):
    if select_data.value == 'Custom Dataset':
        filepath_form = mo.ui.file_browser(initial_path='/', filetypes=['.csv'])
        display(mo.md(f"__Select your dataset .csv file:__ {filepath_form}"))

        blob_containers = list_containers_in_blob(config_dict['connection_string'])
        container_form = mo.ui.dropdown(blob_containers).form()
        display(mo.md(f"__Select your blob container:__ {container_form}"))
    else:
        container_form = None
        filepath_form = None
    return blob_containers, container_form, filepath_form


@app.cell
def __(
    container_form,
    display,
    filepath_form,
    label_list,
    np,
    pd,
    select_data,
):
    if select_data.value == 'Default': 
        run_condition = 1
    elif container_form is not None and filepath_form.path(0) is not None:
        run_condition = 2
    else:
        run_condition = 0
        print("ACTION REQUIRED: Please enter dataset information.")

    if run_condition == 2:
        try: 
            # load the selected file as a pandas dataframe - the file must be a .csv
            loaded_data = pd.read_csv(filepath_form.path(0), index_col=0)
            # create a new column to indicate the container
            loaded_data['container'] = container_form.value
            # decode the names of the probability columns
            new_data = loaded_data.rename(columns=dict(zip(list(np.arange(10).astype(
                       int).astype(str)),label_list)))
            display(new_data.head())
        except: 
            print("ERROR: Unable to process dataset. Please check that the structure matches requirements.")
    elif run_condition == 1:
        naames_location = '../PIVOT/PIVOT/data/naames_subset.csv'
        loaded_data = pd.read_csv(naames_location, index_col=0)
        # create a new column to indicate the container
        loaded_data['container'] = 'naames'
        # decode the names of the probability columns
        new_data = loaded_data.rename(columns=dict(zip(list(np.arange(10).astype(
                   int).astype(str)),label_list)))
        display(new_data.head())
    else:
        new_data = None
    return loaded_data, naames_location, new_data, run_condition


@app.cell
def __(mo):
    mo.md(r"""---""")
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(r"""## Building the SQL database""")
    return


@app.cell(hide_code=True)
def __(sq):
    def build_db(click_value, build_file='../PIVOT/PIVOT/create_db.sql'):
        """ A function to automate the setup of all the tables, stored procedures,
        and variables needed to run PIVOT. 

        :var click_value: A value built into the marimo button to prevent the user
                          from running this function twice in one sitting. Can be 
                          circumvented by re-running the button cell. 
        :type click_value: int
        :var build_file: Points to the create_db.sql file in the PIVOT folder. 
        :type build_file: str, optional
        """
        if click_value < 1:
            # build tables from script
            with open(build_file, 'r', encoding='utf-8') as file:
                sql_string = file.read()
            sq.run_sql_query(sql_string)
            print("INFO: Tables have been constructed.")

            # create stored procedures
            sq.create_alter_stored_procedure('GENERATE_RANDOM_TEST_SET')
            sq.create_alter_stored_procedure('GENERATE_IMAGES_TO_METRIZE')
            sq.create_alter_stored_procedure('GENERATE_IMAGES_TO_PREDICT')
            sq.create_alter_stored_procedure('AL_RANKINGS')
            sq.create_alter_stored_procedure('MODEL_EVALUATION_MAX_CONSENSUS_FILTERING')
            sq.create_alter_stored_procedure('MODEL_EVALUATION_NON_TEST')
            sq.create_alter_stored_procedure('AL_TRAIN_SET')
            sq.create_alter_stored_procedure('METRICS_SELECTION')
            sq.create_alter_stored_procedure('VALIDATION_SET')
            print("INFO: All stored procedures have been built.")

            # create the IDList type for diatom selection
            sq.run_sql_query("CREATE TYPE IDList AS TABLE (Value INT);")
            print("INFO: All custom datatypes have been defined.")
    return build_db,


@app.cell(hide_code=True)
def __(build_check, build_db, mo):
    builb_db_button = mo.ui.button(value=0, on_click=lambda value: value + 1, 
                                   on_change=lambda value: build_db(value, build_file='../PIVOT/PIVOT/create_db.sql'), 
                                   label="Create database structure", disabled=build_check.value)
    builb_db_button
    # This button cannot be clicked twice as a protection against repeat commands. If for some reason you need to run this twice on the same dataset, just re-run the cell and it should work. Alternatively, run commands directly through the SQL database.
    return builb_db_button,


@app.cell
def __(display, mo, sq):
    def check_db_features():
        """ This function looks at the structure of the SQL database and shows features of each table, 
        which stored procedures have been saved, and lists the user-defined variables. 

        :type button: bool, optional
        """
        sql_db = sq.run_sql_query("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE';")

        sql_db['TABLE_NAME']

        for table in sql_db['TABLE_NAME']:        
            table_structure = sq.run_sql_query(f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}';")
            display(mo.md(f'{table} table information:'))
            display(table_structure[['TABLE_NAME', 'COLUMN_NAME', 'ORDINAL_POSITION', 'IS_NULLABLE', 'DATA_TYPE', 
                                     'CHARACTER_MAXIMUM_LENGTH']])

        sps = sq.run_sql_query("SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE';")
        display(mo.md("Stored procedures:"))
        display([sp for sp in sps['SPECIFIC_NAME']])

        user_vars = sq.run_sql_query("SELECT * FROM SYS.TYPES WHERE IS_USER_DEFINED = 1;")
        display(mo.md("User-defined variables:"))
        display(user_vars)
    return check_db_features,


@app.cell
def __(check_db_features, mo):
    check_structure_button = mo.ui.button(value=0, on_click=lambda value: value + 1, on_change=lambda value: check_db_features(), label="Check database structure")

    check_structure_button
    return check_structure_button,


@app.cell
def __(mo):
    mo.md(r"""---""")
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(r"""## Data ingestion""")
    return


@app.cell
def __(display, mo):
    def test_fn():
        print('abc')
        display(mo.md("Will this disappear?"))
    return test_fn,


@app.cell(hide_code=True)
def __(display, mo):
    # images
    display(mo.md("""### Images table"""))
    return


@app.cell(hide_code=True)
def __(display, idu, mo):
    def images_insert(run_condition, df, container_form=None):
        """ Inserts data into the new database with the initial_ingestion function!

        :var run_condition: Tells the function whether the data exists in the notebook and which
                            blob container the images are in.
        :type run_condition: int
        :var df: The dataframe that must include a filepath column
        :type df: DataFrame
        :var container_form: If the user is using their own data, this value indicates the container
                             name of the blob storage.
        :type container_form: marimo._plugins.ui._impl.input.form or NoneType, optional
        """
        if run_condition != 0:
            if run_condition == 1:
                container = 'naames'
            elif run_condition == 2:
                container = container_form.value
            display(mo.md("Beginning data ingestion into the Images table."))

            idu.initial_ingestion(image_filepaths=df.filepath.values, container=container)
            display(mo.md("Complete!"))
        else:
            print("ACTION REQUIRED: Please enter dataset information.")
    return images_insert,


@app.cell(hide_code=True)
def __(
    container_form,
    images_check,
    images_insert,
    mo,
    new_data,
    run_condition,
):
    images_button = mo.ui.button(value=0, on_click=lambda value: value + 1, 
                                 on_change=lambda value: images_insert(run_condition=run_condition, df=new_data, 
                                                                       container_form=container_form), 
                                 label="Insert data to Images table", disabled=images_check.value)
    images_button
    return images_button,


@app.cell(hide_code=True)
def __(display, mo):
    # models
    display(mo.md("### Models table"))

    # load model data
    # insert into table
    return


@app.cell(hide_code=True)
def __(display, mo, run_condition):
    if run_condition != 0:
        model_link_form = mo.ui.text().form()

        display(mo.md("""_Current model link:_  
                      https://basemodel-endpoint.westus2.inference.ml.azure.com/score 
                      """))
        display(mo.md(f"Enter the location of your cloud model: {model_link_form}"))
    return model_link_form,


@app.cell(hide_code=True)
def __(display, du, label_list, mo, np):
    def models_insert(model_name, model_link):
        """ Inserts the model information into the Models table.

        :var model_name: Whatever you want to name the model, unless it conflicts with 
                         a previous model name.
        :type model_name: str
        :var model_link: The cloud location URL of the model (http or https).
        :type model_link: str
        """
        display(mo.md("Beginning data insertion into the Models table."))
        cnn_class_map = dict(zip(list(np.arange(10).astype(int)),label_list))

        du.insert_data(table_name='models', 
                       data = {"model_name": model_name,
                               "model_link": model_link,
                               "class_map": str(cnn_class_map)
                              })
        display(mo.md("Complete!"))
    return models_insert,


@app.cell(hide_code=True)
def __(config_dict, mo, model_link_form, models_check, models_insert):
    models_button = mo.ui.button(value=0, on_click=lambda value: value + 1, 
                                 on_change=lambda value: models_insert(model_name=config_dict['model_name'],
                                                                       model_link=model_link_form.value),
                                 label="Initial Models insert", disabled=models_check.value)
    models_button
    return models_button,


@app.cell(hide_code=True)
def __(display, mo):
    # predictions
    display(mo.md("### Predictions table"))

    # button - calls bulk insert
    return


@app.cell(hide_code=True)
def __(display, idu, mo, np, pd, sq, sql_im_tab):
    def predictions_insert(run_condition, df, m_id, label_list, container_form=None):
        """ Inserts data into the Predictions table.

        :var run_condition: Tells the function whether the data exists in the notebook and which
                            blob container the images are in.
        :type run_condition: int
        :var df: The dataframe. Must have probability columns with column names corresponding to the
                 labels in label_list.
        :type df: DataFrame
        :var m_id: Indicates which model was used to generate the label predictions. Corresponds to 
                   model ID in the Models table.
        :type m_id: int
        :var label_list: A list of all possible predicted labels. 
        :type label_list: list
        :var container_form: If the user is using their own data, this value indicates the container
                             name of the blob storage.
        :type container_form: marimo._plugins.ui._impl.input.form or NoneType, optional
        """
        if run_condition !=0:
            if run_condition == 1:
                container = 'naames'
            elif run_condition == 2:
                container = container_form.value
            # load image ids and add them to 
            display(mo.md("Beginning data insertion into Predictions table."))
            ql_im_tab = sq.run_sql_query(f"select * from images where container = '{container}';")
            df_merged = df.merge(sql_im_tab[sql_im_tab['container'] == container], on='filepath', 
                                 how='inner')

            # combine the class probabilities into 1 dict per image
            prob_dicts = (df_merged[label_list].rename(columns = dict(zip(label_list, 
                          list(np.arange(10).astype(int))))).to_dict(orient = 'records'))
            # Gather the necessary variables in the right order for inserting into the Predictions Table
            pred_table = pd.DataFrame(data=df_merged[['i_id', 'pred_label']])
            pred_table['class_prob'] = np.array(prob_dicts, dtype=str)
            # assign model id as 1 -- this can be changed into a select box when we add more models
            pred_table['m_id'] = m_id  #np.ones(len(pred_table.index))
            # reorder for insertion
            pred_table = pred_table[['m_id', 'i_id', 'class_prob', 'pred_label']]
            # prepare a dictionary for insert
            pred_table_insert = pred_table.to_dict(orient='records')
            # insert into the predictions table
            idu.bulk_insert_data(table_name='predictions', data=pred_table_insert)
            display(mo.md("Complete!"))
        else:
            print("ACTION REQUIRED: Please enter dataset information.")
    return predictions_insert,


@app.cell(hide_code=True)
def __(
    container_form,
    label_list,
    mo,
    new_data,
    predictions_check,
    predictions_insert,
    run_condition,
):
    preds_button = mo.ui.button(value=0, on_click=lambda value: value + 1, 
                                on_change=lambda value: predictions_insert(run_condition=run_condition, df=new_data, 
                                                                           m_id=1, label_list=label_list, 
                                                                           container_form=container_form), 
                                label="Insert data to Predictions table", disabled=predictions_check.value)
    preds_button
    return preds_button,


@app.cell(hide_code=True)
def __(display, mo):
    # dissimilarity
    display(mo.md("### Dissimilarity table"))
    return


@app.cell(hide_code=True)
def __(display, mo, sq):
    def dissimilarity_insert():
        """ Inserts selection methods and their corresponding formulae into the 
        Dissimilarity table. 
        """
        display(mo.md("Beginning data insertion into Dissimilarity table."))
        # insert at index 0 representing random test_data
        sq.run_sql_query(
            """
            SET IDENTITY_INSERT dissimilarity ON;

            INSERT INTO dissimilarity (d_id, name, formula)
            VALUES (0, 'random_sample', 'none');
            SET IDENTITY_INSERT dissimilarity OFF;
            """
        )

        # insert entropy row
        sq.run_sql_query(
            """
            INSERT INTO dissimilarity (name, formula)
            VALUES ('entropy', '-x.T @ np.nan_to_num(np.log(x))');
            """
        )

        # insert diatom selection row
        sq.run_sql_query(
            """
            INSERT INTO dissimilarity (name, formula)
            VALUES ('diatom', 'x >= 0.58');
            """
        )
        display(mo.md("Complete!"))
    return dissimilarity_insert,


@app.cell(hide_code=True)
def __(dissim_check, dissimilarity_insert, mo):
    dissim_button = mo.ui.button(value=0, on_click=lambda value: value + 1, 
                                 on_change=lambda value: dissimilarity_insert(),
                                 label="Initial Dissimilarity insert", disabled=dissim_check.value)
    dissim_button
    return dissim_button,


@app.cell(hide_code=True)
def __(display, mo):
    # metrics
    display(mo.md("### Metrics table"))

    # checkboxes to include which formulas to include - default all
    entropy_check = mo.ui.checkbox(label='Entropy')
    diatom_check = mo.ui.checkbox(label='Diatoms')
    # lcs_check = mo.ui.checkbox()
    # lms_check = mo.ui.checkbox()
    display(mo.md(f"""
    _Select which metrics you want to calculate:_    
    {entropy_check}    
    {diatom_check}  
    """))

    # button to add data to table
    return diatom_check, entropy_check


@app.cell
def __(diatom_check, entropy_check):
    # record values of checkboxes 
    metrics_dict = {0: False,  # Placeholder for the random selection metric (no calc)
                    1: entropy_check.value,
                    2: diatom_check.value
                   }
    return metrics_dict,


@app.cell(hide_code=True)
def __(display, idu, mo, np, pd, sq):
    # Metrics calculation methods
    def least_confident_score(x):
        return 1 - np.max(x)

    def least_margin_score(x):
        sort_x = np.sort(x)
        return 1 - (sort_x[-1] - sort_x[-2])

    def entropy_score(x):
        return -x.T @ np.nan_to_num(np.log(x))

    # Note: diatom selection happens in the app code. The metric for diatom selection is 
    # just the diatom probability. Greater than or equal to 0.58 allows the image to be selected. 

    def get_score(x, f):
        try:
            s = f(x)
        except:
            print(f"Error computing {f.__name__}")
            s = -1
        return s

    def metrics_insert(run_condition, df, metrics_dict, m_id, label_list, container_form=None):
        """ Inserts data into the Metrics table.

        :var run_condition: Tells the function whether the data exists in the notebook and which
                            blob container the images are in.
        :type run_condition: int
        :var df: The dataframe. Must have probability columns with column names corresponding to the
                 labels in label_list.
        :type df: DataFrame
        :var metrics_dict: A dictionary that indicates which metrics calculations to perform on this
                           dataset. 
        :type metrics_dict: dict
        :var m_id: Indicates which model was used to generate the label predictions. Corresponds to 
                   model ID in the Models table.
        :type m_id: int
        :var label_list: A list of all possible predicted labels. 
        :type label_list: list
        :var container_form: If the user is using their own data, this value indicates the container
                             name of the blob storage.
        :type container_form: marimo._plugins.ui._impl.input.form or NoneType, optional
        """
        if run_condition !=0:
            if run_condition == 1:
                container = 'naames'
            elif run_condition == 2:
                container = container_form.value
            # load image ids and add them to df
            display(mo.md("Beginning data insertion into Metrics table."))
            sql_im_tab = sq.run_sql_query(f"select * from images where container = '{container}';")
            df_merged = df.merge(sql_im_tab[sql_im_tab['container'] == container], on='filepath', 
                                 how='inner')

            metrics_table = pd.DataFrame(data=df_merged[['i_id']])

            # loop through all the selected metrics methods
            for key in metrics_dict.keys():
                if metrics_dict[key] is True:
                    metrics_table['m_id'] = m_id
                    metrics_table['d_id'] = key

                    # calculate metrics based on formula associated with key value
                    if key == 1:
                        display(mo.md("Inserting entropy values."))
                        metrics_table['d_value'] = df_merged[label_list].apply(lambda x: get_score(
                            x, entropy_score), axis=1)
                    elif key == 2:
                        display(mo.md("Inserting diatom values."))
                        metrics_table['d_value'] = df_merged['Diatom']

                    # reorder table and convert to a dictionary format
                    metrics_table = metrics_table[['i_id', 'm_id', 'd_id', 'd_value']]
                    metrics_table_insert = metrics_table.to_dict(orient='records')
                    # insert into the metrics table
                    idu.bulk_insert_data(table_name='metrics', data=metrics_table_insert)
            display(mo.md("Complete!"))
        else:
            print("ACTION REQUIRED: Please enter dataset information.")
    return (
        entropy_score,
        get_score,
        least_confident_score,
        least_margin_score,
        metrics_insert,
    )


@app.cell(hide_code=True)
def __(
    container_form,
    label_list,
    metrics_check,
    metrics_dict,
    metrics_insert,
    mo,
    new_data,
    run_condition,
):
    metrics_button = mo.ui.button(value=0, on_click=lambda value: value + 1, 
                                  on_change=lambda value: metrics_insert(run_condition=run_condition, 
                                                                         df=new_data, metrics_dict=metrics_dict,
                                                                         m_id=1, label_list=label_list,
                                                                         container_form=container_form), 
                                  label="Insert data to Metrics table", disabled=metrics_check.value)
    metrics_button
    return metrics_button,


@app.cell
def __(mo):
    mo.md(r"""---""")
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(r"""## Final checks""")
    return


@app.cell
def __(display, mo, pd, sq):
    def run_final_checks(df=None, container_form=None):
        # calculate length of dataset
        if df is not None:
            df_length = len(df.index)
        n_models = 2
        n_dissim = 3
        # indicate container
        if container_form is not None:
            container = container_form.value
        else:
            container = 'naames'
        # Count number of images in the images table that correspond to the container
        im_count = sq.run_sql_query(f"""select count (*) as n from images 
                                        where container = '{container}';""")['n'][0]
        # Count number of entries in the predictions table 
        pred_count = sq.run_sql_query(f"""select count (*) as n from predictions 
                                          inner join images 
                                          on predictions.i_id = images.i_id 
                                          where images.container = '{container}';
                                          """)['n'][0]
        # count number of entries in metrics table
        metrics_count = sq.run_sql_query(f"""select count (*) as n from metrics 
                                             inner join images 
                                             on metrics.i_id = images.i_id 
                                             where images.container = '{container}';
                                             """)['n'][0]
        # count number of models
        model_count = sq.run_sql_query("select count (*) as n from models;")['n'][0]
        # count number of dissimilarity entries
        dissim_count = sq.run_sql_query("select count (*) as n from dissimilarity;")['n'][0]
        # show table comparing expected and actual number of entries for all these things
        if df is not None:
            stats_dict = {'Table_name': ['Images', 'Predictions', 'Metrics', 'Models', 'Dissimilarity'],
                          'Expected_count': [df_length, df_length, df_length*(n_dissim - 1), n_models, n_dissim],
                          'Actual_count': [im_count, pred_count, metrics_count, model_count, dissim_count]
                         }
        else:
            stats_dict = {'Table_name': ['Images', 'Predictions', 'Metrics', 'Models', 'Dissimilarity'],
                          'Count': [im_count, pred_count, metrics_count, model_count, dissim_count]
                         }

        stats_df = pd.DataFrame(data=stats_dict)
        display(mo.md(f"**Entry counts of {container} data by table:**"))
        display(stats_df.head(len(stats_dict['Table_name'])))
        display(mo.md("**Model table entries:**"))
        display(sq.run_sql_query("Select * from models;"))
        display(mo.md("**Dissimilarity table entries:**"))
        display(sq.run_sql_query("Select * from dissimilarity;"))
    return run_final_checks,


@app.cell(hide_code=True)
def __(container_form, mo, new_data, run_final_checks):
    check_contents_button = mo.ui.button(value=0, on_click=lambda value: value + 1, 
                                         on_change=lambda value: run_final_checks(df=new_data, 
                                                                                  container_form=container_form), 
                                         label="Run final checks")
    check_contents_button
    return check_contents_button,


@app.cell
def __(mo):
    mo.md(r"""---""")
    return


if __name__ == "__main__":
    app.run()
