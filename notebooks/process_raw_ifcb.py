import marimo

__generated_with = "0.9.18"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    return (mo,)


@app.cell
def __(mo):
    mo.md(
        """
        # Raw IFCB Data Processing Notebook
        ---
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Instructions

        1. Select your root folder with the file browser. This folder should contain your `raw` folder with the `.hdc`, `.adr`, and `.roi` IFCB files. The root folder should also contain a metadata `.csv` file and a taxonomic grouping `.csv` file. Optionally, it can also have an EcoTaxa classification file.
        2. If you want to include a pre-existing EcoTaxa classification file, select the checkbox, otherwise leave it unchecked.
        3. Enter all the required file name information.
        4. Select whether you want to use default or custom parameters for Science Export. If you select custom, enter information for all the parameters in the user input fields that appear.
        5. Click the 'Run ML preparation' button. This will generate the `ml` folder of `.png`'s needed for future pipeline steps.
        6. Click the 'Run science preparation' button. This step creates a detailed MATLAB table for analysis.
        7. If you're using Azure Blob storage to store data, upload the new `ml` folder to the blob by first entering blob credentials and the name of the new container then clicking the 'Upload to blob' button.

        __Congrats, you're now ready to move on to CNN classification!__  
        Next notebook: `create_dataset.csv`  
        ---
        """
    )
    return


@app.cell
def __():
    # imports
    import numpy as np
    import pandas as pd
    import seaborn as sns
    import matplotlib.pyplot as plt
    import utopia_pipeline_tools.ifcb_data_tools as idt
    import utopia_pipeline_tools.azure_blob_tools as abt

    import os
    from datetime import datetime
    import matlab.engine
    from IPython.display import display
    return abt, datetime, display, idt, matlab, np, os, pd, plt, sns


@app.cell
def __(mo):
    mo.md(rf"## Preprocessing steps")
    return


@app.cell
def __(mo):
    filepath_form = mo.ui.file_browser(initial_path='/', selection_mode='directory', multiple=False, label="Select folder...")
    mo.md(f"""__Select your root folder:__  
    This should be the folder that contains your raw data folder and other reqired files.  
    {filepath_form}""")
    return (filepath_form,)


@app.cell
def __(mo):
    ecotaxa_availability_check = mo.ui.checkbox(label="EcoTaxa Availability")

    mo.md(f"""**Check the box if classification from EcoTaxa is available:**  
    {ecotaxa_availability_check}  
    """)
    return (ecotaxa_availability_check,)


@app.cell
def __(display, ecotaxa_availability_check, mo):
    if ecotaxa_availability_check.value is True:
        metadata_file = mo.ui.text().form()
        taxon_group_file = mo.ui.text().form()
        ecotaxa_file = mo.ui.text().form()
        display(mo.md(f"""  
        **Enter the filenames needed to process the data:**  
        Metadata: {metadata_file}  
        Taxonomic Grouping: {taxon_group_file}   
        EcoTaxa: {ecotaxa_file}  

        _Example files names_  
        Metadata: `metadata.csv`  
        Taxonomic Grouping: `taxonomic_grouping_v5.csv`  
        EcoTaxa: `EcoTaxa_20211013`  
        """))
    else:
        metadata_file = mo.ui.text().form()
        display(mo.md(f"""
        **Enter the filenames needed to process the data:**  
        Metadata: {metadata_file}   

        _Example files_  
        Metadata: `metadata.csv`"""))
    return ecotaxa_file, metadata_file, taxon_group_file


@app.cell
def __(display, mo):
    default_science = mo.ui.checkbox(False, label="Use default values")
    default_info = {'PROJECT_NAME': 'TEST-DATA',
                    'ECOTAXA_EXPORT_DATE': '20211013',
                    'IFCB_RESOLUTION': 2.7488,  # pixels/µm
                    'CALIBRATED': True,  # if True, apply calibration from pixel to µm using the IFCB_RESOLUTION
                    'REMOVED_CONCENTRATED_SAMPLES': False
                   }
    display(mo.md(f"""**Select default or custom Scientific Export parameters:**  
    {default_science}  

    _Default values:_  
    Project name: {default_info['PROJECT_NAME']}  
    EcoTaxa export date (YYYYMMDD): {default_info['ECOTAXA_EXPORT_DATE']}  
    IFCB resolution (pixels/$\mu$m): {default_info['IFCB_RESOLUTION']}  
    Calibrated: {default_info['CALIBRATED']}  
    Removed concentrated samples: {default_info['REMOVED_CONCENTRATED_SAMPLES']}
    """))
    return default_info, default_science


@app.cell
def __(default_science, display, mo):
    if default_science.value is not True:
        project_name = mo.ui.text().form()
        ecotaxa_export_date = mo.ui.text().form()
        ifcb_resolution = mo.ui.text().form()
        calibrated = mo.ui.checkbox(label='Check if True')
        removed_concentrated_samples = mo.ui.checkbox(label='Check if True')
        display(mo.md(f"""**Enter custom parameters:**  
        Project name: {project_name}  
        EcoTaxa export date (YYYYMMDD): {ecotaxa_export_date}  
        IFCB resolution (pixels/$\mu$m): {ifcb_resolution}  
        Calibrated:   
        {calibrated}  
        Removed concentrated samples:   
        {removed_concentrated_samples}  
        """))
    return (
        calibrated,
        ecotaxa_export_date,
        ifcb_resolution,
        project_name,
        removed_concentrated_samples,
    )


@app.cell
def __(
    calibrated,
    default_science,
    ecotaxa_export_date,
    ifcb_resolution,
    project_name,
    removed_concentrated_samples,
):
    if default_science.value is True:
        info = {'PROJECT_NAME': 'TEST-DATA',
                'ECOTAXA_EXPORT_DATE': '20211013',
                'IFCB_RESOLUTION': 2.7488,  # pixels/µm
                'CALIBRATED': True,  # if True, apply calibration from pixel to µm using the IFCB_RESOLUTION
                'REMOVED_CONCENTRATED_SAMPLES': False
               }
        sci_info_entered = True
    else:
        info = {'PROJECT_NAME': project_name.value,
                'ECOTAXA_EXPORT_DATE': ecotaxa_export_date.value,
                'IFCB_RESOLUTION': ifcb_resolution.value,  # pixels/µm
                'CALIBRATED': calibrated.value,  # if True, apply calibration from pixel to µm using the IFCB_RESOLUTION
                'REMOVED_CONCENTRATED_SAMPLES': removed_concentrated_samples.value
               }
        if None not in [project_name.value, ecotaxa_export_date.value, ifcb_resolution.value, 
                        calibrated.value, removed_concentrated_samples.value]: 
            sci_info_entered = True
        else:
            sci_info_entered = False
    return info, sci_info_entered


@app.cell
def __(
    ecotaxa_availability_check,
    ecotaxa_file,
    filepath_form,
    metadata_file,
    os,
    taxon_group_file,
):
    if filepath_form.path() is not None and metadata_file.value is not None:
        # Path to data and files
        root = filepath_form.path()
        path_to_raw_data = os.path.join(root, 'raw')
        path_to_metadata = os.path.join(root, metadata_file.value)
        path_to_ml = os.path.join(root, 'ml')
        path_to_science = os.path.join(root, 'sci')
        if ecotaxa_availability_check.value is True:
            # if classification from EcoTaxa is available
            path_to_classification = os.path.join(root, 'from_ecotaxa/' + ecotaxa_file.value)
            path_to_taxonomic_grouping_csv = os.path.join(root, taxon_group_file.value)
        else:
            path_to_classification = None  # if classification from EcoTaxa not yet available 
            path_to_taxonomic_grouping_csv = None
        info_entered = True

    elif filepath_form.path() is None and metadata_file.value is not None:
        info_entered = False
        print("ACTION REQUIRED: Select your root folder.")
    elif filepath_form.path() is not None and metadata_file.value is None:
        info_entered = False
        print("ACTION REQUIRED: Enter file information.")
    else:
        info_entered = False
        print("ACTION REQUIRED: Select your root folder and enter file information.")
    return (
        info_entered,
        path_to_classification,
        path_to_metadata,
        path_to_ml,
        path_to_raw_data,
        path_to_science,
        path_to_taxonomic_grouping_csv,
        root,
    )


@app.cell
def __(filepath_form, metadata_file, path_to_metadata, pd):
    if filepath_form.path() is not None and metadata_file.value is not None:
        pd.read_csv(path_to_metadata, parse_dates=['DateTime'])
    return


@app.cell
def __(os):
    def prepare_data(bin_extractor, path, purpose, bin_list=None, matlab_info=None):
        if purpose == 'ml':
            bin_extractor.run_machine_learning(output_path=path)
        elif purpose == 'science':
            if bin_list is None:
                bin_list = []
            os.mkdir(path)
            bin_extractor.run_science(output_path=path, bin_list=bin_list, 
                                      update_classification=True, make_matlab_table=True, 
                                      matlab_table_info=matlab_info)
        else:
            print("ACTION REQUIRED: Please enter a valid purpose.")
    return (prepare_data,)


@app.cell
def __(mo):
    mo.md(r"""---""")
    return


@app.cell
def __(mo):
    mo.md(r"""## Run data preparation steps""")
    return


@app.cell
def __(info_entered, mo):
    run_binextractor_button = mo.ui.button(value=False, on_click=lambda value: True, label="Prepare extractor", disabled=not info_entered)

    run_binextractor_button
    return (run_binextractor_button,)


@app.cell
def __(
    display,
    idt,
    matlab,
    path_to_classification,
    path_to_metadata,
    path_to_raw_data,
    path_to_taxonomic_grouping_csv,
    run_binextractor_button,
):
    if run_binextractor_button.value:
        display("Preparing MATLAB and Bin Extractor...")
        matlab_engine = matlab.engine.start_matlab()
        # matlab_engine.parpool()
        ifcb = idt.BinExtractor(path_to_raw_data, path_to_metadata, path_to_classification, path_to_taxonomic_grouping_csv,
                                matlab_parallel_flag=True , matlab_engine=matlab_engine)
        display("Preparation complete!")
    return ifcb, matlab_engine


@app.cell
def __(ifcb, info_entered, mo, path_to_ml, prepare_data):
    run_ml_button = mo.ui.button(value=0, on_click=lambda value: value + 1, on_change=lambda value:
                                 prepare_data(ifcb, path_to_ml, 'ml'), label="Run ML preparation", 
                                 disabled=not info_entered)

    run_ml_button
    return (run_ml_button,)


@app.cell
def __(
    ifcb,
    info,
    info_entered,
    mo,
    path_to_science,
    prepare_data,
    sci_info_entered,
):
    run_sci_button = mo.ui.button(value=0, on_click=lambda value: value + 1, on_change=lambda value:
                                  prepare_data(ifcb, path_to_science, 'science', matlab_info=info),
                                  label="Run science preparation", disabled=not info_entered or not sci_info_entered)

    run_sci_button
    return (run_sci_button,)


@app.cell
def __(mo):
    mo.md(r"""---""")
    return


@app.cell
def __(mo):
    mo.md(r"""## Upload `ml` folder to blob storage""")
    return


@app.cell
def __(mo):
    # specify new container name
    container_form = mo.ui.text().form()
    mo.md(f"Specify new container name: {container_form}")
    return (container_form,)


@app.cell
def __(abt):
    def upload_to_new_container(container_name, path_to_ml):
        abt.create_container(container_name)
        print(f"INFO: New container '{container_name}' has been created!")
        abt.upload_images_to_blob(container_name, path_to_ml)
        print(f"INFO: Images uploaded successfully.")
    return (upload_to_new_container,)


@app.cell
def __(
    container_form,
    filepath_form,
    metadata_file,
    mo,
    path_to_ml,
    upload_to_new_container,
):
    # button to upload!
    upload_to_blob_button = mo.ui.button(value=0, on_click=lambda value: value + 1, on_change=lambda value: 
                                         upload_to_new_container(container_form.value, path_to_ml),
                                         label="Create new container and upload images", disabled=(
                                             container_form.value is None or metadata_file.value 
                                             is None or filepath_form.value is None))

    upload_to_blob_button
    return (upload_to_blob_button,)


@app.cell
def __(mo):
    mo.md(r"""---""")
    return


if __name__ == "__main__":
    app.run()
