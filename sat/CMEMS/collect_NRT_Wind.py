from cmapingest import vault_structure as vs
import os

"""

FINISHED --2021-06-15 16:53:37--
Total wall clock time: 5h 45m 21s
Downloaded: 5085 files, 51G in 5h 1m 57s (2.86 MB/s)

"""
output_dir = vs.collected_data + "sat/CMEMS_NRT_Wind/"
output_dir = output_dir.replace(" ", "\\ ")
ftp_link = "ftp://nrt.cmems-du.eu/Core/WIND_GLO_WIND_L4_NRT_OBSERVATIONS_012_004/CERSAT-GLO-BLENDED_WIND_L4-V6-OBS_FULL_TIME_SERIE/*"


def wget_file(fpath, output_dir, usr, psw):
    os.system(
        f"""wget -nd -r -m --ftp-user={usr} --ftp-password={psw} {fpath}  -P  {output_dir}"""
    )


wget_file(ftp_link, output_dir, "{insert_usr", "insert_psw")
