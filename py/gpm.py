from ftplib import FTP, error_perm
from datetime import datetime
import pandas as pd
from pandas import DataFrame
import os
from time import sleep
import calendar
import xarray as xr

from_time = '2018-11-19'
from_time_dt = datetime(*(int(i) for i in from_time.split('-')))

if os.path.exists('file_df.pkl'):
    file_df = pd.read_pickle('file_df.pkl')
else:
    file_df = DataFrame({'time': [], 'path': []}).set_index('time')

# log into FTP server
login = 'david.brochart@gmail.com'
url = 'jsimpson.pps.eosdis.nasa.gov'

def ftp_login(url, login):
    logged_in = False
    while not logged_in:
        ftp = FTP(url, user=login, passwd=login)
        try:
            ftp.login()
            logged_in = True
        except error_perm as e:
            if str(e).endswith('You are already logged in'):
                logged_in = True
            else:
                print(e)
                try:
                    ftp.quit()
                except:
                    pass
                print(f'Could not log into {url}')
                seconds = 60
                print(f'Will retry in {seconds} seconds')
                sleep(seconds)
        print(f'Logged into {url}')
    return ftp

ftp = ftp_login(url, login)

# change to IMERG "early run" estimate directory
ftp.cwd('NRTPUB/imerg/early')

# get list of directories, one directory per month (e.g. 201801)
dir_list = []
ftp.dir(dir_list.append)
yearmonth_list = [line.split()[-1] for line in dir_list]

times = []
paths = []
for yearmonth in yearmonth_list:
    year = int(yearmonth[:4])
    month = int(yearmonth[4:])
    # check if we need to enter this directory
    # if the next month is present in the DataFrame,
    # we already downloaded all the data.
    # if the month is before from_tim, we don't
    # want to download the data
    enter_monthdir = False
    if datetime(year, month, calendar.monthrange(year, month)[1]) < from_time_dt:
        pass
    elif len(file_df) == 0:
        enter_monthdir = True
    else:
        if month == 12:
            if len(file_df.loc[f'{year+1}-01':]) == 0:
                enter_monthdir = True
        else:
            if len(file_df.loc[f'{year}-{str(month+1).zfill(2)}':]) == 0:
                enter_monthdir = True
    if enter_monthdir:
        ftp.cwd(yearmonth)
        dir_list = []
        ftp.dir(dir_list.append)
        filename_list = [line.split()[-1] for line in dir_list]
        for filename in filename_list:
            s = 'IMERG.'
            if s in filename:
                i0 = filename.find(s) + len(s)
                i1 = filename[i0:].find('.') + i0
                time = filename[i0:i1]
                year = int(time[:4])
                month = int(time[4:6])
                day = int(time[6:8])
                hour = int(time[10:12])
                minu = int(time[12:14]) # start of the 30-minute measurement
                minu += 15 # middle of the measurement time range
                t = datetime(year, month, day, hour, minu)
                if t >= from_time_dt:
                    # get the files that we have not already downloaded
                    append_file = False
                    if len(file_df) == 0:
                        if t >= from_time_dt:
                            append_file = True
                    else:
                        if t not in file_df.loc[from_time:].index:
                            append_file = True
                    if append_file:
                        path = f'{yearmonth}/{filename}'
                        print(f'Will download {path}')
                        times.append(t)
                        paths.append(path)
        ftp.cwd('..')
ftp.quit()

newfile_df = DataFrame({'time': times, 'path': paths}).set_index('time')
newfile_df = newfile_df.sort_index()

if len(newfile_df) > 0:
    ftp = ftp_login(url, login)
    ftp.cwd('NRTPUB/imerg/early')
    for path in newfile_df.loc[from_time:, 'path'].values:
        fname = f'tmp/{os.path.basename(path)}'
        if os.path.exists(fname):
            print(f'Already downloaded: {path}')
        else:
            print(f'Downloading {path}')
            with open(fname, 'wb') as f:
                ftp.retrbinary(f'RETR {path}', f.write)
    ftp.quit()
    file_df = pd.concat([file_df, newfile_df])
    file_df.to_pickle('file_df.pkl')

if False:
    ds = []
    index = file_df.loc['2018-11-19 00:00:00':'2018-11-19 03:00:00', 'path'].index
    for time in index.values:
        path = f'tmp/{os.path.basename(file_df.loc[time, "path"])}'
        d = xr.open_dataset(path, engine='pynio')
        d = d.rename({name:name.replace('Grid/', '') for name in d.data_vars})
        d = d.set_coords(['lon', 'lat'])
        a = d.attrs
        d.attrs = {k.replace('Grid/', ''):v for k, v in a.items()}
        ds.append(d)
    
    ds = xr.concat(ds, index)
    
    ds.to_zarr('zarr')
