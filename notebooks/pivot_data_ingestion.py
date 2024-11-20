import marimo

__generated_with = "0.8.17"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    return mo,


@app.cell(hide_code=True)
def __(mo):
    mo.md(r"""# PIVOT Data Ingestion""")
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        __Adding New Data to an Existing Database__

        Follow these steps to add new data to a database that is already fully set up!

        1. Select your container from the drop-down menu and your data file from the file browser.
        2. Wait for the notebook to finish running - it's preparing your data to be inserted.
        3. Check for warnings and insert into Images
        4. Insert into Predictions
        5. Insert into Metrics

        __Message Key__

        - `INFO` tells you information about the actions taken by the cell. No user action required.
        - `ACTION REQUIRED` indicates you haven't filled out a prerequisite input in the notebook. Most likely, your container or data file hasn't been selected yet.
        - `WARNING` means something has gone wrong unrelated to the data, typically with the SQL server. Check your database availability.
        - `ERROR` tells you when something has gone wrong with the data. Check that your dataset structure matches the requirements and if you've correctly mapped the probability values with the predicted labels.
        """
    )
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(r"""## Setup Steps""")
    return


@app.cell
def __():
    # imports
    # INFO: You may get a streamlit runtime warning, this can be ignored. Run the cell again if you want it to disappear.
    import numpy as np
    import pandas as pd
    # import seaborn as sns
    import matplotlib.pyplot as plt
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
        plt,
        reload,
        sq,
        sys,
        tqdm,
        trange,
        upt,
    )


@app.cell
def __(upt):
    label_dict = upt.label_dict
    label_list = upt.label_list
    return label_dict, label_list


@app.cell
def __(list_containers_in_blob, mo):
    # retreive all available blobs from the Azure blob storage and make them a list
    #config = load_config()
    #connection_string = config['connection_string']

    connection_string = 'DefaultEndpointsProtocol=https;AccountName=ifcbwestus;AccountKey=bHLeaeVSCT4DK7nwzof/lTmKctp6+ljI9e95efaZTM8klc6MwMseApKN9qRT8GXFxwLnroDtvXGE+ASt14eASA==;EndpointSuffix=core.windows.net'
    blob_containers = list_containers_in_blob(connection_string)

    container_form = mo.ui.dropdown(blob_containers).form()
    mo.md(f"__Select your blob container:__ {container_form}")
    return blob_containers, connection_string, container_form


@app.cell
def __(mo):
    filepath_form = mo.ui.file_browser(initial_path='/', filetypes=['.csv'])
    mo.md(f"__Select your dataset .csv file:__ {filepath_form}")
    return filepath_form,


@app.cell
def __(container_form, filepath_form, sq):
    try:
        # setting conditions to disable the images ingestion button
        if container_form.value is not None:
            # if the container has been selected, check whether the container has already been added to the sal database's images table
            image_count = sq.run_sql_query(f"SELECT COUNT(1) FROM dbo.images WHERE container = '{container_form.value}';")
            ingestion_disabled = bool(image_count.iloc[0,0] > 10)  # the 10 here is arbitrary, can be adjusted if we need to add more images from 
                                                                   # the same container separately
        else: 
            # ingestion is also disabled if the container hasn't been selected
            ingestion_disabled = True

        # setting the run conditions for all buttons and code that requires information from the container and file forms
        if container_form.value is not None and filepath_form.path(0) is not None: 
            # container and file must both be selected
            run_condition = True
        else:
            run_condition = False
    except:
        print("WARNING: Unable to connect to the database")
    return image_count, ingestion_disabled, run_condition


@app.cell(hide_code=True)
def __(mo):
    mo.md(r"""## Insert into Images""")
    return


@app.cell
def __(
    container_form,
    display,
    filepath_form,
    label_list,
    np,
    pd,
    run_condition,
    sq,
):
    if run_condition is True:
        # load the selected file as a pandas dataframe - the file must be a .csv
        loaded_data = pd.read_csv(filepath_form.path(0), index_col=0)
        # create a new column to indicate the container
        loaded_data['container'] = container_form.value
        # decode the names of the probability columns
        new_data = loaded_data.rename(columns=dict(zip(list(np.arange(10).astype(
                   int).astype(str)),label_list)))
        # load the images table from the sql database
        try: 
            images_table = sq.run_sql_query("select * from images;")
        except:
            print("WARNING: Unable to connect to database")

    try:
        display(new_data.head())
    except: 
        print("ACTION REQUIRED: Select blob container and dataset file")

    if run_condition is True:
        print("Running data ingestion checks and preparing dataset for insert:")
        # Test to see whether the maximum value of the predictions matches up with the predicted label column -- should be True
        pred_match = np.all(loaded_data[list(np.arange(10).astype(int).astype(str))].idxmax(
                            axis=1).astype(int) == loaded_data.pred_label)

        # load images table and print its shape
        print("Shape of SQL images table:", images_table.shape)

        # checking whether the database has existing data
        if images_table.shape[0] > 1:
            empty_db = False
        else:
            empty_db = True
            print("INFO: Images table is empty")

        # if the probabilities match with the predicted label, prepare the data for ingestion, otherwise print an error
        if pred_match:
            if empty_db is False:
                images_table_insert = loaded_data[['filepath', 'container']].to_dict(orient='records')
            else:
                images_table_insert = loaded_data

            print("Dataset is ready for ingestion!")
            print("Shape of new dataset: ", new_data.shape)
        else:
            print("ERROR: Predicted labels do not match the CNN probability outputs")
    return (
        empty_db,
        images_table,
        images_table_insert,
        loaded_data,
        new_data,
        pred_match,
    )


@app.cell(disabled=True)
def __(np):
    def run_data_ingestion_checks(dataframe, img_table):
        """ This function prepares the input dataframe to be ingested into the images table of the sql
        database. It checks whether the pred_label column aligns with the probability values and 
        determines whether the to use initial ingestion (when the database has no existing entries) or 
        a bulk insert. 

        :param dataframe: The dataframe to be ingested. Must have probability (0 - 9), filepath, and 
                          container columns.
        :type dataframe: DataFrame
        :param img_table: An input to avoid re-loading the sql db images table (since that takes a 
                          while)
        :type img_table: DataFrame
        """
        # Test to see whether the maximum value of the predictions matches up with the predicted label column -- should be True
        pred_match = np.all(dataframe[list(np.arange(10).astype(int).astype(str))].idxmax(
                            axis=1).astype(int) == dataframe.pred_label)

        # load images table and print its shape
        images_table = img_table
        print("Shape of images table:", images_table.shape)

        # checking whether the database has existing data
        if images_table.shape[0] > 0:
            empty_db = False
        else:
            empty_db = True
            print("INFO: Images table is empty")

        # if the probabilities match with the predicted label, prepare the data for ingestion, otherwise print an error
        if pred_match:
            if empty_db is False:
                images_table_insert = dataframe[['filepath', 'container']].to_dict(orient='records')
            else:
                images_table_insert = dataframe

            print("Dataset is ready for ingestion!")
        else:
            print("ERROR: Predicted labels do not match the CNN probability outputs")
        return (images_table_insert, empty_db)
    return run_data_ingestion_checks,


@app.cell(disabled=True)
def __(
    images_table,
    loaded_data,
    mo,
    run_condition,
    run_data_ingestion_checks,
):
    # button calls the run_data_ingestion_checks to perform checks and prepare the dataframe for ingestion 
    mo.ui.button(value = None, on_click=lambda value: loaded_data, on_change=lambda value:
                 run_data_ingestion_checks(value, img_table=images_table), label='Run checks', 
                 disabled=bool(run_condition is False))
    return


@app.cell
def __(container_form, idu):
    def insert_to_table(click_val, table_name, input_data, db_status=False):
        """ This function inserts a prepared dataframe into a table in the SQL database on the 
        click of a button.

        :param click_val: A value used to run this function only when the button has been pressed
        :type click_val: int
        :param table_name: The dataframe to be inserted, must be the output of the 
                           run_data_ingestion_checks or equivalent
        :type table_name: dictionary
        :param input_data: The dataframe to be inserted, must be the output of the 
                           run_data_ingestion_checks or equivalent; data type depends on initial 
                           ingestion vs. bulk insert decision
        :type input_data: list or DataFrame
        :param db_status: Indicates whether the SQL database has existing entries, True means it is 
                          empty. This kwarg is required to initiate initial data ingestion
        :type db_status: bool, optional
        """
        # functions run if the button has been pressed (click_value is True)
        if click_val == 1:
            # Use a different function for initial ingestion of the images table of an empty dataframe
            if table_name == 'images' and db_status is True:
                idu.initial_ingestion(image_filepaths=input_data.filepath.values, 
                                      container=container_form.value) # STILL NEED TO FIX FN!!!!
            else:
                idu.bulk_insert_data(table_name=table_name, data=input_data) # No need to add an i_id column for 
                                                                        # images table insert, it's generated 
                                                                        # automatically!
    return insert_to_table,


@app.cell
def __(
    empty_db,
    images_table_insert,
    ingestion_disabled,
    insert_to_table,
    mo,
):
    images_button = mo.ui.button(value=0, on_click=lambda value: value + 1, on_change=lambda value: 
                                 insert_to_table(value, 'images', images_table_insert,
                                 db_status=empty_db), label='Insert data to Images table', 
                                 disabled=ingestion_disabled)
    images_button
    # This button cannot be clicked twice as a protection against repeat data ingestions. If for some reason you need to run this twice on the same dataset, just re-run the cell and it should work.
    return images_button,


@app.cell
def __(container_form, images_button, new_data, sq):
    if images_button.value > 0:
        new_im_count = sq.run_sql_query(f"SELECT COUNT(1) FROM dbo.images WHERE container = '{container_form.value}';")

        if len(new_data.index) == new_im_count.iloc[0,0]:
            print("INFO: All images added!")
        else:
            print(new_im_count.iloc[0,0], "of", len(new_data.index), "images added.")
    return new_im_count,


@app.cell(hide_code=True)
def __(mo):
    mo.md(r"""## Insert into Predictions""")
    return


@app.cell
def __(
    container_form,
    display,
    image_count,
    images_table,
    label_list,
    new_data,
    np,
    pd,
    run_condition,
    sq,
):
    if run_condition is True:
        if bool(image_count.iloc[0,0] < 1):
            # if this is the first predictions input with this container, need to reload the images 
            # table to get the image ids
            reloaded_images_table = sq.run_sql_query("select * from images;")
            new_data_merged = new_data.merge(reloaded_images_table[reloaded_images_table[
                                             'container'] == container_form.value], on='filepath', 
                                             how='inner')
        else:
            # use the existing images table in memory to merge in the image ids
            new_data_merged = new_data.merge(images_table[images_table['container'] == 
                                             container_form.value], on='filepath', how='inner')

        # combine the class probabilities into 1 dict per image
        prob_dicts = (new_data_merged[label_list].rename(columns = dict(zip(label_list, 
                      list(np.arange(10).astype(int))))).to_dict(orient = 'records'))
        # Gather the necessary variables in the right order for inserting into the Predictions Table
        pred_table = pd.DataFrame(data=new_data_merged[['i_id', 'pred_label']])
        pred_table['class_prob'] = np.array(prob_dicts, dtype=str)
        # assign model id as 1 -- this can be changed into a select box when we add more models
        pred_table['m_id'] = 1  #np.ones(len(pred_table.index))
        # reorder for insertion
        pred_table = pred_table[['m_id', 'i_id', 'class_prob', 'pred_label']]
        # prepare a dictionary for insert
        pred_table_insert = pred_table.to_dict(orient='records')

    try:
        display(pred_table.head())
    except:
        print("ACTION REQUIRED: Select blob container and dataset file")
    return (
        new_data_merged,
        pred_table,
        pred_table_insert,
        prob_dicts,
        reloaded_images_table,
    )


@app.cell
def __(insert_to_table, mo, pred_table_insert):
    mo.ui.button(value=0, on_click=lambda value: value + 1, on_change=lambda value: 
                 insert_to_table(value, 'predictions', pred_table_insert), 
                 label='Insert data to Predictions table')
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Adding a Model

        - Add a model to the table
        - Add the model to wherever the models are stored online
        - Connect to the blob and run predictions for all existing datasets for the model -- then insert data into predictions and metrics
        - Maybe have a checkbox setting to select which datasets to run the new model on?
        """
    )
    return


@app.cell(disabled=True)
def __(sq):
    # insert 0 representing test_data
    sq.run_sql_query(
    """
    SET IDENTITY_INSERT models ON;


    INSERT INTO models (m_id, model_name, model_link, class_map)
    VALUES (0, 'random_sample', 'none', 'none');
    SET IDENTITY_INSERT models OFF;
    """
    )
    return


@app.cell(disabled=True)
def __(du, label_list, np):
    cnn_label_map = dict(zip(list(np.arange(10).astype(int)),label_list))

    du.insert_data(table_name='models', data = {"model_name":"model-cnn-v1-b3",
                                                "model_link": "https://basemodel-endpoint.westus2.inference.ml.azure.com/score",
                                                "class_map": str(cnn_label_map)
    })
    return cnn_label_map,


@app.cell(hide_code=True)
def __(mo):
    mo.md(r"""## Adding to the Dissimilarity Table?""")
    return


@app.cell(disabled=True)
def __(sq):
    # insert 0 representing random test_data
    sq.run_sql_query(
    """
    SET IDENTITY_INSERT dissimilarity ON;

    INSERT INTO dissimilarity (d_id, name, formula)
    VALUES (0, 'random_sample', 'none');
    SET IDENTITY_INSERT dissimilarity OFF;
    """
    )

    sq.run_sql_query(
    """
    INSERT INTO dissimilarity (name, formula)
    VALUES ('entropy', '-x.T @ np.nan_to_num(np.log(x))')
    """
    )
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(r"""## Insert into Metrics""")
    return


@app.cell
def __(np):
    def least_confident_score(x):
        return 1 - np.max(x)

    def least_margin_score(x):
        sort_x = np.sort(x)
        return 1 - (sort_x[-1] - sort_x[-2])

    def entropy_score(x):
        return -x.T @ np.nan_to_num(np.log(x))

    def get_score(x, f):
        try:
            s = f(x)
        except:
            print(f"Error computing {f.__name__}")
            s = -1
        return s
    return (
        entropy_score,
        get_score,
        least_confident_score,
        least_margin_score,
    )


@app.cell
def __(new_data_merged):
    # merge exports csv data with images table data to retrieve image ids
    diatom_table = new_data_merged[['i_id', 'Diatom']]
    diatom_table['m_id'] = 1
    diatom_table['d_id'] = 2
    diatom_table.rename(columns={'Diatom':'d_value'}, inplace=True)
    diatom_table = diatom_table[['i_id', 'm_id', 'd_id', 'd_value']]
    diatom_table_insert = diatom_table.to_dict(orient='records')
    return diatom_table, diatom_table_insert


@app.cell
def __(diatom_table_insert, empty_db, insert_to_table, mo):
    diatom_button = mo.ui.button(value=0, on_click=lambda value: value + 1, on_change=lambda value: 
                                 insert_to_table(value, 'metrics', diatom_table_insert,
                                 db_status=empty_db), label='Insert diatom data to Metrics table')
    diatom_button
    return diatom_button,


if __name__ == "__main__":
    app.run()
