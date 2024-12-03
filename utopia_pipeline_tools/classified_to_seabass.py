""" Classified to SeaBASS Data Format Module

This module contains (FNs/Classes?) to convert CNN-classified and/or validated 
datasets to a SeaBASS-compatible format. 
"""

import pandas as pd
import numpy as np
import utopia_pipeline_tools as upt

# write a function/class to convert :)
class MakeSeaBASS():
    """
    """
    def __init__(self, filepath, location='blob', investigator_info=None, 
                 experiment=None, cruise=None, stations=False, 
                 measurement_depth=5, doc_list=None, data_status='final', 
                 trigger_mode='both', notes=None):
        """
        - location can be blob or local
        - input dict of same format as default investigators for investigator_info
        - trigger mode can be chlorophyll, scattering, or both
        - notes optional
        """
        if location == 'blob':
            # open file with blob container client
            # read csv and save as self.df or something
            self.sample_df = pd.read_csv(...)
        elif location == 'local':
            self.sample_df = pd.read_csv(filepath)

        # derive sample_ID, date, time from filename
        sample_ID, date, time, ifcb_number = self.extract_sample_info(filepath)

        if investigator_info is not None:
            investigators, affiliations, emails = self.extract_investigator_info(dictionary=investigator_info)
        else:
            investigators, affiliations, emails = self.extract_investigator_info()

        r2r_event = None #???????????????

        if stations is False:
            station = 'NA'
        else:
            station = None # figure out where you'd get station information???

        latitude = '!!!' # get from metadata values (first non-nan or None)
        longitude = '!!!' # also get from metadata values ""
        volume_sampled = 5 # but actually get this from the csv ""
        volume_imaged = '!!!' # also get this from the csv ""
        pixel_per_um = '!!!' # get this from the csv too ""

        filename = f"""{experiment.value.replace(" ", "")}-{
            cruise.value.replace(" ", "")}_{
                r2r_event}_discrete_IFCB_plankton_and_particles_{date}.sb"""

        link_to_blob_location = 'blob_link.html' # figure out the format of these links and derive from sample name and credential dict
        tgz_filename = 'raw_file_name.tgz' # get name of this pls - maybe from sample name??? Or something else??

        if notes is not None:
            self.notes_bool = True
            self.notes = notes
        else:
            self.notes_bool = False

        self.header_values = {'id_product_DOI': None,
                              'investigators': investigators,
                              'affiliations': affiliations,
                              'emails': emails,
                              'experiment': experiment.replace(" ", ""),
                              'cruise': cruise.replace(" ", ""),
                              'station': station,
                              'r2r_event': r2r_event,
                              'filename': filename,
                              'documents': doc_list,
                              'calibration_file': None,
                              'eventID': sample_ID,
                              'data_type': 'flow_thru',
                              'ifcb_number': ifcb_number,
                              'data_status': data_status.lower(),
                              'date': date,
                              'time': time,
                              'latitude': latitude,
                              'longitude': longitude,
                              'water_depth': None,
                              'measurement_depth': measurement_depth,
                              'volume_sampled': volume_sampled,
                              'volume_imaged': volume_imaged,
                              'pixel_per_um': pixel_per_um,
                              'blob_location_link': link_to_blob_location,
                              'assiciated_archives': tgz_filename,
                              'associated_archive_types': 'raw',
                              'length_representation_instrument_varname': 'maxFeretDiameter',
                              'width_representation_instrument_varname': 'minFeretDiameter',
                              'missing': None,
                              'delimeter': 'comma',
                              'ifcb_trigger_mode': trigger_mode
                              }
    
    def compile_header(self):
        """
        """
        if self.header_values['ifcb_trigger_mode'] == 'both':
            trigger_note = '''chlorophyll fluorescence (PMTB) OR side scattering (PMTA)'''
        elif self.header_values['ifcb_trigger_mode'] == 'chlorophyll':
            trigger_note = 'chlorophyll fluorescence (PMTB)'
        elif self.header_values['ifcb_trigger_mode'] == 'scattering':
            trigger_note = 'side scattering (PMTA)'

        header = f"""
                  /begin_header
                  ...
                  """

        if self.notes_bool:
            header = header + f"""\n!\n! {self.notes} \n!"""

    def extract_sample_info(self, filepath):
        """ Uses the filepath to extract information about the sample.

        :param filepath: Filepath to the sample folder.
        :type filepath: str

        Returns:
        :param sample_ID: Includes a string of letters and numbers representing 
        the date and time of when the sample was taken and the IFCB instrument 
        number of the IFCB used to collect the sample.
        :type sample_ID: str
        :param date: Date in the form DDMMYYYY.
        :type date: str
        :param time: Time of collection in the 24-hr form HHMMSS.
        :type time: str
        :param ifcb_number: IFCB number, assumes the instrument number has three
        digits. If it has more or less, adjust the n_digits value in the code.
        :type ifcb_number: str
        """
        # write some code to extract the sample ID, date, and time from the filepath name
        if '/' in filepath:
            filepath = filepath.split('/')[-1]
        elif '\\' in filepath:
            filepath = filepath.split('\\')[-1]
        
        apres_D = filepath.split('D')[1]
        apres_T = filepath.split('T')[1]

        sample_ID = filepath.split('.')[0]
        date = ''.join(list(apres_D)[0:8])
        time = ''.join(list(apres_T)[0:6])

        n_digits = 3
        ifcb_number = ''.join(list(filepath.split('IFCB')[-1])[0:n_digits])

        return sample_ID, date, time, ifcb_number
    
    def extract_investigator_info(self, dictionary=upt.default_investigators):
        """ Retrieves investigator information from the dictionary in the 
        __init__.py file of utopia_pipeline_tools (or optionally from a manually
        specified dictionary). 

        :param dictionary: Dictionary containing investigator names, 
        affiliations, and emails.
        :type dictionary: dict, optional

        Returns:
        :param investigators: Comma-separated string of investigators with no 
        spaces.
        :type investigators: str
        :param affiliations_list: Comma-separated string of affiliated 
        organizations with no spaces.
        :type affiliations_list: str
        :param emails_list: Comma-separated string of the investigators' emails 
        with no spaces.
        :type emails_list: str
        """
        investigators = ",".join(list(dictionary.keys())).replace(" ", "_")
        affiliations_list = []
        emails_list = []
        for n in np.arange(len(dictionary.values())):
            affiliations_list.append(list(
                dictionary.values())[n][0])
            emails_list.append(list(
                dictionary.values())[n][1])
        
        affiliations = ",".join(affiliations_list).replace(" ", "_")
        emails = ",".join(emails_list).replace(" ", "_")
        
        return investigators, affiliations, emails