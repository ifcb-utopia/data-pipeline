import marimo

__generated_with = "0.9.18"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    return (mo,)


@app.cell
def __():
    from utopia_pipeline_tools.azure_blob_tools import list_containers_in_blob
    from IPython.display import display
    import utopia_pipeline_tools as upt
    import numpy as np
    return display, list_containers_in_blob, np, upt


@app.cell
def __():
    # select metadata and predicted labels csvs
    return


@app.cell
def __(mo):
    blob_selection = mo.ui.checkbox()
    default_investigators = mo.ui.checkbox()
    mo.md(f"""Select if your images are stored on the Azure blob:   
    {blob_selection}  
    Using default investigators:   
    {default_investigators}
    """)
    return blob_selection, default_investigators


@app.cell
def __(
    blob_selection,
    default_investigators,
    display,
    list_containers_in_blob,
    mo,
):
    experiment = mo.ui.text()
    if blob_selection.value:
        cruise = mo.ui.dropdown(list_containers_in_blob())
    else:
        cruise = mo.ui.text()
    if default_investigators.value:
        display(mo.md(f"""__Enter header information:__  
                       Experiment name: {experiment}  
                       Cruise: {cruise}  
               """))
    else:
        investigators = mo.ui.text()
        orgs = mo.ui.text()
        emails = mo.ui.text()
        display(mo.md(f"""__Enter header information:__  
                       Please type all investigators, associated organizations, and emails in comma-separated 
                       lists. Investigators should be of the form: _First Last, First Last, ..._  \n
                       Experiment name: {experiment}  
                       Cruise: {cruise}   
                       Investigators: {investigators}  
                       Organizations: {orgs}  
                       Emails: {emails}
                """))
    return cruise, emails, experiment, investigators, orgs


@app.cell
def __(upt):
    test = list(upt.default_investigators.values())
    #upt.default_investigators[test[0]][0]
    test[1][0]
    len(test)
    return (test,)


@app.cell
def __(np, upt):
    id_product_DOI = None

    affiliations_list = []
    emails_list = []
    for n in np.arange(len(upt.default_investigators.values())):
        affiliations_list.append(list(upt.default_investigators.values())[n][0])
        emails_list.append(list(upt.default_investigators.values())[n][1])

    station = None
    r2r_event = None
    ...
    return (
        affiliations_list,
        emails_list,
        id_product_DOI,
        n,
        r2r_event,
        station,
    )


@app.cell
def __(cruise):
    cruise.value
    return


@app.cell
def __(
    affiliations_list,
    cruise,
    emails_list,
    experiment,
    id_product_DOI,
    station,
    upt,
):
    header  = f"""
    /begin_header
    /identifier_product_doi={id_product_DOI}
    /investigators={",".join(list(upt.default_investigators.keys()))}
    /affiliations={",".join(affiliations_list)}
    /contact={",".join(emails_list)}
    /experiment={experiment.value}
    /cruise={cruise.value}
    /station={station}
    """
    return (header,)


app._unparsable_cell(
    r"""
    /begin_header
    /identifier_product_doi=10.5067/SeaBASS/EXPORTS/DATA001
    /investigators=Lee_Karp-Boss,Heidi_Sosik
    /affiliations=University_of_Maine,Woods_Hole_Oceanographic_Institution
    /contact=lee.karp-boss@maine.edu,hsosik@whoi.edu
    /experiment=EXPORTS
    /cruise=EXPORTSNP
    /station=-9999
    /r2r_event=RR1813-SE-20180813.1915.001
    /data_file_name=EXPORTS-EXPORTSNP_RR1813_discrete_IFCB_plankton_and_particles_20180813194123_R2.sb
    /documents=IFCB_brief_protocol_Sosik_Oct2021.docx,checklist_IFCB_plankton_and_particles_EXPORTS-EXPORTSNP_RR1813_R2_Sosik_automated_classification.docx,namespace_ptwg_nonconforming_roi_v1.yml,automated_assessed_id_EXPORTSNP_IFCB107.txt
    /calibration_files=no_cal_files
    /eventID=D20180813T194123_IFCB107
    /data_type=flow_thru
    /instrument_model=Imaging_FlowCytobot_IFCB107
    /instrument_manufacturer=McLane_Research_Laboratories_Inc
    /data_status=final
    /start_date=20180813
    /end_date=20180813
    /start_time=19:15:00[GMT]
    /end_time=19:15:00[GMT]
    /north_latitude=49.9041[DEG]
    /south_latitude=49.9041[DEG]
    /east_longitude=-142.6099[DEG]
    /west_longitude=-142.6099[DEG]
    /water_depth=NA
    /measurement_depth=5
    /volume_sampled_ml=5
    /volume_imaged_ml=2.6735
    /pixel_per_um=2.77
    /associatedMedia_source=http://ifcb-data.whoi.edu/EXPORTS/D20180813T194123_IFCB107.html
    /associated_archives=EXPORTS-EXPORTSNP_RR1813_IFCB_raw_data_associated.tgz
    /associated_archive_types=raw
    /length_representation_instrument_varname=maxFeretDiameter
    /width_representation_instrument_varname=minFeretDiameter
    /missing=-9999
    /delimiter=comma
    !
    ! EXPORTSNP cruise RR1813
    !
    ! underway_discrete; concentration: 133X
    !
    ! IFCB trigger mode: chlorophyll fluorescence (PMTB) OR side scattering (PMTA)
    !
    ! To access each image directly from the associatedMedia string: replace .html with .png
    !
    ! v4 ifcb-analysis image products; https://github.com/hsosik/ifcb-analysis
    !
    ! Files updated (R2 version) to include taxonomic classification
    !
    /fields=associatedMedia,data_provider_category_automated,scientificName_automated,scientificNameID_automated,prediction_score_automated_category,biovolume,area_cross_section,length_representation,width_representation,equivalent_spherical_diameter
    /units=none,none,none,none,none,um^3,um^2,um,um,um
    /end_header
    """,
    name="__"
)


if __name__ == "__main__":
    app.run()
