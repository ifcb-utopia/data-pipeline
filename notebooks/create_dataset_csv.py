import marimo

__generated_with = "0.9.18"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def __(mo):
    mo.md(r"""# Creating the Dataset .csv File From Images on the Azure Blob""")
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        __Steps for generating the .csv file of image filepaths, classification probabilities, and predicted labels__  

        1. Enter the Azure Blob connection string in the text box (no apostrophes!).
        2. Select which container you want to classify from the drop-down menu that appears after you submit the connection string.
        3. Click the "Retrieve filepaths" button to get the image filepaths from the blob storage. This will take a few minutes for smaller datasets (500,000 images) or about an hour for larger datasets (10,000,000 images). 
        4. Click the "Predict labels" button to classify the images with the CNN. This will take about 4 hours per 1,000,000 images. I recommend running this portion overnight. If the dataset is particularly large, this may require a VM since a personal computer's working memory can be too small to handle massive datasets. For instance, after 10 hours of runtime on the taraeuropa23 dataset (almost 10,000,000 images), my computer gave me a memory error. 
        5. If no errors occur, a new .csv file has been saved to your local machine!
        """
    )
    return


@app.cell
def __():
    # imports
    import numpy as np
    import pandas as pd

    from azure.storage.blob import ContainerClient

    import cv2

    from tensorflow import keras
    from IPython.display import display
    from utopia_pipeline_tools.azure_blob_tools import list_containers_in_blob, list_files_in_blob
    from utopia_pipeline_tools.ifcb_data_tools import retrieve_filepaths_from_local
    from utopia_pipeline_tools.cnn_tools import load_local_model, preprocess_input
    return (
        ContainerClient,
        cv2,
        display,
        keras,
        list_containers_in_blob,
        list_files_in_blob,
        load_local_model,
        np,
        pd,
        preprocess_input,
        retrieve_filepaths_from_local,
    )


@app.cell
def __(mo):
    mo.md(r"""## Setup""")
    return


@app.cell
def __(mo):
    # form to select whether your model is local or on the cloud
    folder_location_selectbox = mo.ui.dropdown(options=['local', 'cloud'], label="Folder location").form()

    mo.md(f"__Select `ml` folder location:__ {folder_location_selectbox}")
    return (folder_location_selectbox,)


@app.cell
def __(display, folder_location_selectbox, mo):
    if folder_location_selectbox.value == 'local':
        filepath_form = mo.ui.file_browser(initial_path='/', selection_mode='directory', multiple=False, label="Select folder...")
        display(mo.md(f"""__Select your root folder:__  
        This should be the `ml` folder that contains folders of images.  
        {filepath_form}"""))
    return (filepath_form,)


@app.cell
def __(display, folder_location_selectbox, mo):
    if folder_location_selectbox.value == 'local':
        dataset_name_form = mo.ui.text().form()
        display(mo.md(f"""__Enter the name of the dataset:__  
        This will be used to name the final `.csv` file.  
        {dataset_name_form}"""))
    return (dataset_name_form,)


@app.cell
def __(mo):
    mo.md(
        r"""
        _ifcbwestus connection string:_  
        DefaultEndpointsProtocol=https;AccountName=ifcbwestus;AccountKey=bHLeaeVSCT4DK7nwzof/
        lTmKctp6+ljI9e95efaZTM8klc6MwMseApKN9qRT8GXFxwLnroDtvXGE+ASt14eASA==;EndpointSuffix=core.windows.net
        """
    )
    return


@app.cell
def __(display, folder_location_selectbox, mo):
    if folder_location_selectbox.value == 'cloud':
        # make a text input form for the user to enter the connection string
        cstr_form = mo.ui.text(label="Connection string").form()
        display(mo.md(f"__Enter your Azure Blob connection string:__ {cstr_form}"))
    return (cstr_form,)


@app.cell
def __(
    cstr_form,
    display,
    folder_location_selectbox,
    list_containers_in_blob,
    mo,
):
    if folder_location_selectbox.value == 'cloud':
        if cstr_form.value is not None:
            # retrieve list of blob containers
            blob_containers = list_containers_in_blob(cstr_form.value)

            # make a drop-down form to select the container
            container_form = mo.ui.dropdown(blob_containers).form()

        try:
            display(mo.md(f"__Select your blob container:__ {container_form}"))
        except:
            print("ACTION REQUIRED: Enter your blob connection string")
    return blob_containers, container_form


@app.cell
def __(
    container_form,
    cstr_form,
    filepath_form,
    folder_location_selectbox,
):
    # setting the run conditions for all buttons and code that requires information from the container and connection string forms
    try: 
        if folder_location_selectbox.value == 'local':
            if filepath_form.value is not None:
                run_condition = True
            else:
                run_condition = False
        elif folder_location_selectbox.value == 'cloud':
            if container_form.value is not None and cstr_form.value is not None: 
            # container and connection string must both be defined
                run_condition = True
            else:
                run_condition = False
        else:
            run_condition = False
    except:
        run_condition = False
    return (run_condition,)


@app.cell(hide_code=True)
def __(mo):
    mo.md(r"""## Retrieve Filepaths from the Azure Blob""")
    return


@app.cell
def __(mo, run_condition):
    # button to run the cells that retrieve filepaths from the blob and organize them in a dataframe
    filepath_retrieval_button = mo.ui.run_button(label="Retrieve filepaths", 
                                                 disabled=bool(run_condition is False))
    filepath_retrieval_button
    return (filepath_retrieval_button,)


@app.cell
def __(
    container_form,
    cstr_form,
    display,
    filepath_form,
    filepath_retrieval_button,
    folder_location_selectbox,
    list_files_in_blob,
    pd,
    retrieve_filepaths_from_local,
):
    if filepath_retrieval_button.value:
        # call the blob to list all the files in the container
        if folder_location_selectbox.value == 'cloud':
            png_df = list_files_in_blob(container_form.value, cstr_form.value)
        elif folder_location_selectbox.value == 'local':
            png_df = pd.DataFrame({'filepath': retrieve_filepaths_from_local(filepath_form.path())})

        display(png_df.head())
    return (png_df,)


@app.cell
def __(mo):
    mo.md(r"""## Classify with the CNN""")
    return


@app.cell
def __(mo):
    # form to select whether your model is local or on the cloud
    model_location_selectbox = mo.ui.dropdown(options=['local', 'cloud'], label="Model location").form()

    mo.md(f"__Select model location:__ {model_location_selectbox}")
    return (model_location_selectbox,)


@app.cell
def __(mo):
    mo.md(
        r"""
        __Current local model filepaths:__  
        ifcbUTOPIA_dev/model-cnn-v1-b3.h5  
        ifcbUTOPIA_dev/model-cnn-v1-b3.json  

        __Current cloud model connection information:__  
        subscription_id: 91804dbe-1fd2-4384-8b66-2b5e4ad1f2f2  
        resource_group: UTOPIA  
        workspace_name: pivot  
        experiment_name: adt-pivot  
        api_key: 4B2DTMyhNHgIk3lJtt8MSdBU5QodpCNf  
        model_name: basemodel  
        endpoint_name: basemodel-endpoint  
        deployment_name: pivot-basemodel
        """
    )
    return


@app.cell
def __(display, mo, model_location_selectbox):
    if model_location_selectbox.value == 'local':
        json_filepath = mo.ui.text(label='.json filepath').form()
        h5_filepath = mo.ui.text(label='.h5 filepath').form()

        display(mo.md(f"""__Enter the filepath locations of the .json and .h5 model files:__   
                {json_filepath}   
                {h5_filepath}
                """))

    elif model_location_selectbox.value == 'cloud':
        sub_id = mo.ui.text(label='Subscription ID')
        resource_group = mo.ui.text(label='Resource group')
        workspace_name = mo.ui.text(label='Workspace name')
        experiment_name = mo.ui.text(label='Experiment name')
        api_key = mo.ui.text(label='API key')
        model_name = mo.ui.text(label='Model name')
        endpoint_name = mo.ui.text(label='Endpoint name')
        deployment_name = mo.ui.text(label='Deployment name')

        display(mo.md(f"""__Enter the filepath locations of the .json and .h5 model files:__   
                {sub_id}  
                {resource_group}  
                {workspace_name}  
                {experiment_name}  
                {api_key}  
                {model_name}  
                {endpoint_name}  
                {deployment_name}  
                """))

    else:
        print("ACTION REQUIRED: Please select the model location")
    return (
        api_key,
        deployment_name,
        endpoint_name,
        experiment_name,
        h5_filepath,
        json_filepath,
        model_name,
        resource_group,
        sub_id,
        workspace_name,
    )


@app.cell(hide_code=True)
def __(
    api_key,
    deployment_name,
    endpoint_name,
    experiment_name,
    h5_filepath,
    json_filepath,
    model_location_selectbox,
    model_name,
    resource_group,
    sub_id,
    workspace_name,
):
    if model_location_selectbox.value == 'local':
        if json_filepath.value == '' or h5_filepath == '':
            model_entry = False
        else:
            model_entry = True
    elif model_location_selectbox.value == 'cloud':
        if (sub_id.value == '' or resource_group.value == '' or workspace_name.value == '' or 
            experiment_name.value == '' or api_key.value == '' or model_name.value == '' or 
            endpoint_name.value == '' or deployment_name.value == ''):
            model_entry = False
        else:
            model_entry = True
    else:
        model_entry = False
    return (model_entry,)


@app.cell(hide_code=True)
def __(mo, model_entry, run_condition):
    # button to run the images through the CNN to predict labels
    prediction_button = mo.ui.run_button(label="Predict labels", 
                                         disabled=bool(run_condition is False or model_entry is False))
    prediction_button
    return (prediction_button,)


@app.cell
def __(
    ContainerClient,
    container_form,
    cstr,
    cv2,
    filepath_retrieval_button,
    folder_location_selectbox,
    h5_filepath,
    json_filepath,
    load_local_model,
    np,
    pd,
    png_df,
    prediction_button,
    preprocess_input,
):
    # classify with model

    # png_df_2 = pd.read_csv("ifcbUTOPIA_dev/taraeuropa23_filepaths.csv", index_col=0)

    if prediction_button.value:
        # connecting to the specific container
        if filepath_retrieval_button.value == 'cloud':
            container_client = ContainerClient.from_connection_string(conn_str=cstr,
                                                                      container_name=container_form.value)
        # load the model from the local files
        v1_b3_model = load_local_model(json_filepath.value, h5_filepath.value)

        # set up lists, counters, and splits
        data_index = np.arange(len(png_df.index))
        if len(png_df.index) < 500000:
            n_splits = int(len(png_df.index)/10000)
        else:
            n_splits = int(len(png_df.index)/500000)
        data_split = np.array_split(data_index, n_splits)
        test_preds = []
        error_list = []
        counter = 0

        # looping through 10 subsets of the data
        for subset in data_split: 
            # set up lists
            image_list = []
            filepaths = []

            # looping through each image in the subset
            for i in subset: 
                # select the row in the dataset and get the filepath
                input_path = png_df.iloc[i]['filepath'] 

                if folder_location_selectbox.value == 'cloud':
                    # call the image from the blob
                    image_download = container_client.download_blob(blob=input_path)
                    image = cv2.imdecode(np.frombuffer(image_download.readall(), dtype=np.uint8),
                                         cv2.IMREAD_UNCHANGED)
                elif folder_location_selectbox.value == 'local':
                    image = cv2.imread(input_path, cv2.IMREAD_UNCHANGED)

                # preprocess the image
                try:
                    processed = np.array(preprocess_input(image))
                    image_list.append(processed)
                    filepaths.append(input_path)
                except:
                    error_list.append((counter, i))
                    

            # predict the labels for all images in the subset
            pred_input = np.array(image_list)
            probabilities = v1_b3_model.predict(pred_input)

            # make a dataframe of the probabilities, filepaths, and predicted labels
            pred_frame = pd.DataFrame(probabilities)
            pred_frame['filepath'] = filepaths
            pred_frame['pred_label'] = [np.argmax(x) for x in probabilities]

            # append the dataframe to the list
            test_preds.append(pred_frame)

            counter += 1 
            print('completed ', str(counter), ' of', n_splits, ' testing subsets')

            del image_list
            del filepaths
            del pred_input
            del probabilities
            del pred_frame


        # concatenate all of the subset dataframes
        test_eval = pd.concat(test_preds)
    return (
        container_client,
        counter,
        data_index,
        data_split,
        error_list,
        filepaths,
        i,
        image,
        image_download,
        image_list,
        input_path,
        n_splits,
        pred_frame,
        pred_input,
        probabilities,
        processed,
        subset,
        test_eval,
        test_preds,
        v1_b3_model,
    )


@app.cell
def __(
    container_form,
    dataset_name_form,
    display,
    folder_location_selectbox,
    test_eval,
):
    try:
        # save the final dataframe as a csv to the local machine
        if folder_location_selectbox.value == 'cloud':
            test_eval.to_csv(f"{container_form.value}_prediction_results.csv")
        elif folder_location_selectbox.value == 'local':
            test_eval.to_csv(f"biotherm_prediction_results.csv")
        display(test_eval.head())
    except:
        if folder_location_selectbox.value == 'local' and dataset_name_form.value is None:
            print("ACTION REQUIRED: Enter dataset name")
        else:
            pass
    return


if __name__ == "__main__":
    app.run()
