# DEV NOTe JAN 21ST: from https://argo.ucsd.edu/data/data-faq/#RorD, only use
# The _prof suffix contains *all* profile data. In the future, use wget? to get not all profiles


import urllib
import glob
import os
import ftplib
import pandas as pd
from cmapingest import vault_structure as vs
from tqdm import tqdm


base_argo_ftp = "ftp://ftp.ifremer.fr/ifremer/argo/"
base_argo_collected_data = vs.collected_data + "ARGO/"
argo_bgc_collected_data = vs.collected_data + "ARGO/BGC/"
argo_core_collected_data = vs.collected_data + "ARGO/Core/"
argo_index_dir = vs.collected_data + "ARGO/Index/"

arglobal = "ar_index_global_prof.txt"
arsyn = "argo_synthetic-profile_index.txt"


def gather_index_from_ftp():
    wget_file(base_argo_ftp + arglobal, argo_index_dir)
    wget_file(base_argo_ftp + arbio, argo_index_dir)
    wget_file(base_argo_ftp + arsyn, argo_index_dir)


def return_basename_from_filepath_list(filepath_list):
    basenamelist = [os.path.basename(x) for x in filepath_list]
    return basenamelist


def get_float_ID_from_vault(directory):
    """returns list of all files in directory"""
    filelist = glob.glob(directory + "*.nc")
    basenamelist = return_basename_from_filepath_list(filelist)
    vault_float_ids = pd.Series(basenamelist).str.split("_", expand=True)[0].to_list()
    return vault_float_ids


def get_float_ID_and_daac_from_vault(directory):
    """returns list of all files in directory"""
    filelist = glob.glob(directory + "**/*.nc", recursive=True)
    if len(filelist) > 0:
        filelist_df = pd.DataFrame({"filepath": filelist})
        daac_float_id_df = (
            filelist_df["filepath"].str.rsplit("/", n=2, expand=True).iloc[:, 1:3]
        )
        daac_and_id = (
            daac_float_id_df.iloc[:, 0].astype(str)
            + "/"
            + daac_float_id_df.iloc[:, 1]
            .str.split("_", n=2, expand=True)
            .iloc[:, 0]
            .astype(str)
        )
    else:
        daac_and_id = []

    return daac_and_id


def get_float_ID_from_index(index_filepath):
    df = pd.read_csv(index_filepath, skiprows=8, sep=",")
    # splits the file column to only get the float ID
    filelist = df["file"].str.split("/", n=3, expand=True)[1]
    basenamelist = return_basename_from_filepath_list(filelist)
    return basenamelist


def get_float_ID_and_daac_from_index(index_filepath):
    df = pd.read_csv(index_filepath, skiprows=8, sep=",")
    # splits the file column to get the float ID and DAAC
    daac_float_id_df = df["file"].str.split("/", n=3, expand=True).iloc[:, 0:2]
    daac_and_id = (
        daac_float_id_df.iloc[:, 0].astype(str)
        + "/"
        + daac_float_id_df.iloc[:, 1].astype(str)
    )
    return daac_and_id


def wget_file(fpath, output_dir):
    os.system("wget " + fpath + " -P " + output_dir)


def diff_index_vault(argo_dir, index_filepath):
    """This function takes in either BGC or Core and checks if there are mismatches/missing files between the index file and the files in vault"""
    # index_ids = get_float_ID_from_index(index_filepath)
    # vault_ids = get_float_ID_from_vault(argo_dir)
    index_ids = get_float_ID_and_daac_from_index(index_filepath)
    vault_ids = get_float_ID_and_daac_from_vault(argo_dir)
    diff_set = list(set(index_ids) ^ set(vault_ids))

    return diff_set


# def format_ids_to_download_str():

# syn_diff = diff_index_vault(argo_bgc_collected_data, argo_index_dir + arsyn)
# core_diff = diff_index_vault(argo_core_collected_data, argo_index_dir + arglobal)


# argo_dir = argo_bgc_collected_data
# index_filepath = argo_index_dir + arsyn


def wget_file_diff(file_diff, argo_dir, float_prefix):
    for fil in tqdm(file_diff):
        daac_name = fil.split("/")[0]
        float_id = fil.split("/")[1]
        fpath = (
            base_argo_ftp
            + "dac/"
            + daac_name
            + "/"
            + float_id
            + "/"
            + float_id
            + "_"
            + float_prefix
            + "prof.nc"
        )
        output_dir = argo_dir + daac_name + "/"
        print(fpath)
        wget_file(fpath, output_dir)


def main_download(argo_float_type):
    """ Main download function. Checks through relevent index to retrieve float_ID's, checks against files that exist. Downloads any missing"""
    if argo_float_type.lower() == "core":
        float_prefix = ""
        index_fname = "ar_index_global_prof.txt"
        argo_dir = base_argo_collected_data + "Core/"
    elif argo_float_type.lower() == "bgc":
        float_prefix = "S"
        index_fname = "argo_synthetic-profile_index.txt"
        argo_dir = base_argo_collected_data + "BGC/"
    else:
        print("ARGO input float type not found. exiting... ")
        sys.exit()
    file_diff = diff_index_vault(argo_dir, argo_index_dir + index_fname)

    wget_file_diff(file_diff, argo_dir, float_prefix)

    # return file_diff


main_download("core")
# file_diff = main_download('bgc')
# f_test = file_diff[0]
# daac_name = f_test.split("/")[0]
# float_id = f_test.split("/")[1]
# fpath = base_argo_ftp + "dac/" + daac_name + "/"+float_id +"/" + float_id +"_Sprof.nc"

# wget_file(fpath,float_id + '_test_file.nc')


# globaldf = pd.read_csv(arglobal, skiprows = 8,sep=',')
# biodf = pd.read_csv(arbio, skiprows = 8,sep=',')
# syndf = pd.read_csv(arsyn, skiprows = 8,sep=',')


"""
FTP_directory = "ftp://usgodae.org/pub/outgoing/argo/dac/"
output_directory = "Vault/collected_data/ARGO/"
FTP transfer used: filezilla
"""

# import pycmap

# api = pycmap.API()
# argo_cols = api.columns('tblArgoMerge_REP')["Columns"].to_list()

# existing_cols = ['float_id','cycle','time','lat','lon','depth','position_qc','direction','data_mode','data_centre','argo_merge_cdnc','argo_merge_cdnc_qc','argo_merge_cdnc_adj','argo_merge_cdnc_adj_qc','argo_merge_cdnc_adj_err','argo_merge_pressure','argo_merge_pressure_qc','argo_merge_pressure_adj','argo_merge_pressure_adj_qc','argo_merge_pressure_adj_err','argo_merge_salinity','argo_merge_salinity_qc','argo_merge_salinity_adj','argo_merge_salinity_adj_qc','argo_merge_salinity_adj_err','argo_merge_temperature','argo_merge_temperature_qc','argo_merge_temperature_adj','argo_merge_temperature_adj_qc','argo_merge_temperature_adj_err','argo_merge_O2','argo_merge_O2_qc','argo_merge_O2_adj','argo_merge_O2_adj_qc','argo_merge_O2_adj_err','argo_merge_bbp','argo_merge_bbp_qc','argo_merge_bbp_adj','argo_merge_bbp_adj_qc','argo_merge_bbp_adj_err','argo_merge_bbp470','argo_merge_bbp470_qc','argo_merge_bbp470_adj','argo_merge_bbp470_adj_qc','argo_merge_bbp470_adj_err','argo_merge_bbp532','argo_merge_bbp532_qc','argo_merge_bbp532_adj','argo_merge_bbp532_adj_qc','argo_merge_bbp532_adj_err','argo_merge_bbp700','argo_merge_bbp700_qc','argo_merge_bbp700_adj','argo_merge_bbp700_adj_qc','argo_merge_bbp700_adj_err','argo_merge_turbidity','argo_merge_turbidity_qc','argo_merge_turbidity_adj','argo_merge_turbidity_adj_qc','argo_merge_turbidity_adj_err','argo_merge_cp','argo_merge_cp_qc','argo_merge_cp_adj','argo_merge_cp_adj_qc','argo_merge_cp_adj_err','argo_merge_cp660','argo_merge_cp660_qc','argo_merge_cp660_adj','argo_merge_cp660_adj_qc','argo_merge_cp660_adj_err','argo_merge_chl','argo_merge_chl_qc','argo_merge_chl_adj','argo_merge_chl_adj_qc','argo_merge_chl_adj_err','argo_merge_cdom','argo_merge_cdom_qc','argo_merge_cdom_adj','argo_merge_cdom_adj_qc','argo_merge_cdom_adj_err','argo_merge_NO3','argo_merge_NO3_qc','argo_merge_NO3_adj','argo_merge_NO3_adj_qc','argo_merge_NO3_adj_err','argo_merge_bisulfide','argo_merge_bisulfide_qc','argo_merge_bisulfide_adj','argo_merge_bisulfide_adj_qc','argo_merge_bisulfide_adj_err','argo_merge_ph','argo_merge_ph_qc','argo_merge_ph_adj','argo_merge_ph_adj_qc','argo_merge_ph_adj_err','argo_merge_down_irr','argo_merge_down_irr_qc','argo_merge_down_irr_adj','argo_merge_down_irr_adj_qc','argo_merge_down_irr_adj_err','argo_merge_down_irr380','argo_merge_down_irr380_qc','argo_merge_down_irr380_adj','argo_merge_down_irr380_adj_qc','argo_merge_down_irr380_adj_err','argo_merge_down_irr412','argo_merge_down_irr412_qc','argo_merge_down_irr412_adj','argo_merge_down_irr412_adj_qc','argo_merge_down_irr412_adj_err','argo_merge_down_irr443','argo_merge_down_irr443_qc','argo_merge_down_irr443_adj','argo_merge_down_irr443_adj_qc','argo_merge_down_irr443_adj_err','argo_merge_down_irr490','argo_merge_down_irr490_qc','argo_merge_down_irr490_adj','argo_merge_down_irr490_adj_qc','argo_merge_down_irr490_adj_err','argo_merge_down_irr555','argo_merge_down_irr555_qc','argo_merge_down_irr555_adj','argo_merge_down_irr555_adj_qc','argo_merge_down_irr555_adj_err','argo_merge_up_irr','argo_merge_up_irr_qc','argo_merge_up_irr_adj','argo_merge_up_irr_adj_qc','argo_merge_up_irr_adj_err','argo_merge_up_irr412','argo_merge_up_irr412_qc','argo_merge_up_irr412_adj','argo_merge_up_irr412_adj_qc','argo_merge_up_irr412_adj_err','argo_merge_up_irr443','argo_merge_up_irr443_qc','argo_merge_up_irr443_adj','argo_merge_up_irr443_adj_qc','argo_merge_up_irr443_adj_err','argo_merge_up_irr490','argo_merge_up_irr490_qc','argo_merge_up_irr490_adj','argo_merge_up_irr490_adj_qc','argo_merge_up_irr490_adj_err','argo_merge_up_irr555','argo_merge_up_irr555_qc','argo_merge_up_irr555_adj','argo_merge_up_irr555_adj_qc','argo_merge_up_irr555_adj_err','argo_merge_down_par','argo_merge_down_par_qc','argo_merge_down_par_adj','argo_merge_down_par_adj_qc','argo_merge_down_par_adj_err','year','month','week','dayofyear']


# argo_dir = vs.collected_data + "ARGO/"
# bodc_dir = argo_dir + "bodc/"
# # bodc_float_list = glob.glob(bodc_dir + "*")
# # for fil in bodc_float_list:
# #     fils_indir = os.listdir(fil)
# #     print(fils_indir)

# ex_float = "/home/nrhagen/Vault/collected_data/ARGO/bodc/1900083/"


# xdf_master = xr.open_dataset(ex_float + "1900083_Rtraj.nc")
# xdf_prof = xr.open_dataset(ex_float + "1900083_prof.nc")
# xdf_meta = xr.open_dataset(ex_float + "1900083_meta.nc")
# xdf_tech = xr.open_dataset(ex_float + "1900083_tech.nc")
# # traj_master = xr.open_mfdataset(ex_float + "profiles/*")

# traj_f1 = xr.open_dataset(ex_float + "profiles/D1900083_025.nc")
# traj_f2 = xr.open_dataset(ex_float + "profiles/D1900083_008.nc")


# # for profile in range(dict(xdf_master.dims)["N_MEASUREMENT"]):
# #     traj_df =xdf_master.isel(N_MEASUREMENT=profile).to_dataframe().reset_index()
# #     trdf = traj_df[["JULD","LATITUDE","LONGITUDE","TEMP","PRES"]]
# #     print(trdf.TEMP.unique())
# master_df = pd.DataFrame(columns = ['N_CALIB','N_HISTORY','N_LEVELS','N_PARAM','N_PROF','DATA_TYPE','FORMAT_VERSION','HANDBOOK_VERSION','REFERENCE_DATE_TIME','DATE_CREATION','DATE_UPDATE','PLATFORM_NUMBER','PROJECT_NAME','PI_NAME','STATION_PARAMETERS','CYCLE_NUMBER','DIRECTION','DATA_CENTRE','DC_REFERENCE','DATA_STATE_INDICATOR','DATA_MODE','PLATFORM_TYPE','FLOAT_SERIAL_NO','FIRMWARE_VERSION','WMO_INST_TYPE','JULD','JULD_QC','JULD_LOCATION','LATITUDE','LONGITUDE','POSITION_QC','POSITIONING_SYSTEM','VERTICAL_SAMPLING_SCHEME','CONFIG_MISSION_NUMBER','PROFILE_PRES_QC','PROFILE_PSAL_QC','PROFILE_TEMP_QC','PRES','PSAL','TEMP','PRES_QC','PSAL_QC','TEMP_QC','PRES_ADJUSTED','PSAL_ADJUSTED','TEMP_ADJUSTED','PRES_ADJUSTED_QC','PSAL_ADJUSTED_QC','TEMP_ADJUSTED_QC','PRES_ADJUSTED_ERROR','PSAL_ADJUSTED_ERROR','TEMP_ADJUSTED_ERROR','PARAMETER','SCIENTIFIC_CALIB_EQUATION','SCIENTIFIC_CALIB_COEFFICIENT','SCIENTIFIC_CALIB_COMMENT','SCIENTIFIC_CALIB_DATE','HISTORY_INSTITUTION','HISTORY_STEP','HISTORY_SOFTWARE','HISTORY_SOFTWARE_RELEASE','HISTORY_REFERENCE','HISTORY_DATE','HISTORY_ACTION','HISTORY_PARAMETER','HISTORY_START_PRES','HISTORY_STOP_PRES','HISTORY_PREVIOUS_VALUE','HISTORY_QCTEST'])
# profile_list = glob.glob(ex_float + "profiles/*")
# for aprofile in profile_list:
#     df = xr.open_dataset(aprofile).to_dataframe().reset_index()
#     master_df = master_df.append(df)

# df = xdf_master.to_dataframe().reset_index()
# df.columns= df.columns.str.lower()
# df = df[['n_cycle',
# #  'n_history',
#  'n_history2',
#  'n_measurement',
#  'n_param',
#  'data_type',
#  'format_version',
#  'handbook_version',
#  'reference_date_time',
#  'platform_number',
#  'project_name',
#  'pi_name',
#  'trajectory_parameters',
#  'data_centre',
#  'date_creation',
#  'date_update',
#  'data_state_indicator',
#  'inst_reference',
#  'wmo_inst_type',
#  'positioning_system',
#  'data_mode',
#  'dc_reference',
#  'juld',
#  'juld_qc',
#  'latitude',
#  'longitude',
#  'position_accuracy',
#  'position_qc',
#  'cycle_number',
#  'pres',
#  'pres_qc',
#  'pres_adjusted',
#  'pres_adjusted_qc',
#  'pres_adjusted_error',
#  'temp',
#  'temp_qc',
#  'temp_adjusted',
#  'temp_adjusted_qc',
#  'temp_adjusted_error',
#  'psal',
#  'psal_qc',
#  'psal_adjusted',
#  'psal_adjusted_qc',
#  'psal_adjusted_error',
#  'juld_ascent_start',
#  'juld_ascent_start_status',
#  'juld_ascent_end',
#  'juld_ascent_end_status',
#  'juld_descent_start',
#  'juld_descent_start_status',
#  'juld_descent_end',
#  'juld_descent_end_status',
#  'juld_start_transmission',
#  'juld_start_transmission_status',
#  'grounded',
#  'history_institution',
#  'history_step',
#  'history_software',
#  'history_software_release',
#  'history_reference',
#  'history_date',
#  'history_action',
#  'history_parameter',
#  'history_previous_value',
#  'history_index_dimension',
#  'history_start_index',
#  'history_stop_index',
#  'history_qctest']
# ]


#     ###############

#     ########## TODO: exclude the profiles in the greylist
#     # indexFname = cfgv.index_argo_raw + argo.grey_csv
#     # greyDF = pd.read_csv(indexFname)
#     #####################################################
#     return mergeDF


# gdac = "ftp://usgodae.org/pub/outgoing/argo/"
# dac = gdac + "dac/"

# ### original unzipped files
# grey = "ar_greylist.txt"
# meta = "ar_index_global_meta.txt.gz"
# tech = "ar_index_global_tech.txt.gz"
# prof = "ar_index_global_prof.txt.gz"
# traj = "ar_index_global_traj.txt.gz"
# merge = "argo_merge-profile_index.txt.gz"
# bio_prof = "argo_bio-profile_index.txt.gz"
# bio_traj = "argo_bio-traj_index.txt.gz"

# ######## csv filenames
# grey_csv = grey[:-3] + "csv"
# meta_csv = meta[:-6] + "csv"
# tech_csv = tech[:-6] + "csv"
# prof_csv = prof[:-6] + "csv"
# traj_csv = traj[:-6] + "csv"
# merge_csv = merge[:-6] + "csv"
# bio_prof_csv = bio_prof[:-6] + "csv"
# bio_traj_csv = bio_traj[:-6] + "csv"
