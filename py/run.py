import click
import pickle
from ftplib import FTP, error_perm
import numpy as np
from datetime import date, datetime, timedelta
import calendar
from time import sleep
import os
import pandas as pd
from pandas import DataFrame
from zipfile import ZipFile
from osgeo import gdal
from gr4j import gr4j
import h5py
import grid

def resize(data, ratio):
    """Resize 'data' by resampling with 'ratio'."""
    new_data = np.zeros(tuple(int(i * ratio) for i in data.shape))
    if ratio < 1:
        data_nb = np.zeros(tuple(int(i * ratio) for i in data.shape))
        for y in range(data.shape[0]):
            for x in range(data.shape[1]):
                new_y = int(y * ratio)
                new_x = int(x * ratio)
                new_data[new_y, new_x] += data[y, x]
                data_nb[new_y, new_x] += 1
        new_data /= data_nb
    else:
        for y in range(new_data.shape[0]):
            for x in range(new_data.shape[1]):
                new_data[y, x] = data[int(y // ratio), int(x // ratio)]
    return new_data

class Log:
    """Logging to a file and/or to stdout."""
    def __init__(self, file_path=None, printout=False):
        self.printout = printout
        if file_path is None:
            self.f = None
        else:
            self.f = open(file_path, 'wt')
    def write(self, string):
        if self.printout:
            print(string)
        if self.f is not None:
            self.f.write(string)
            self.f.flush()

def ftp_login(url, login, wd, log, seconds=60):
    """Try to log into 'url' using 'login' for user and passwd.
    Retry every 'seconds'."""
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
                log.write(f'{e}')
                try:
                    ftp.quit()
                except:
                    pass
                log.write(f'Could not log into {url}')
                log.write(f'Will retry in {seconds} seconds')
                sleep(seconds)
        log.write(f'Logged into {url}')
    ftp.cwd(wd)
    log.write(f'Changed directory to {wd}')
    return ftp

def get_new_files(login, url, log, ftp_dst_dir, from_time, reset=False):
    if reset or not os.path.exists(f'{ftp_dst_dir}/file_df.pkl'):
        file_df = DataFrame({'time': [], 'dir': [], 'name': []}).set_index('time')
        file_df.to_pickle(f'{ftp_dst_dir}/file_df.pkl')
    file_df = pd.read_pickle(f'{ftp_dst_dir}/file_df.pkl')
    from_time_dt = datetime(*(int(i) for i in from_time.split('-')))

    # log into FTP server
    ftp = ftp_login(url, login, 'NRTPUB/imerg/early', log)

    # get list of directories, one directory per month (e.g. 201801)
    dir_list = []
    ftp.dir(dir_list.append)
    yearmonth_list = [line.split()[-1] for line in dir_list]

    times = []
    dirs = []
    names = []
    for yearmonth in yearmonth_list:
        year = int(yearmonth[:4])
        month = int(yearmonth[4:])
        # check if we need to enter this directory
        # if the next month is present in the DataFrame,
        # we already downloaded all the data of this month.
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
                            log.write(f'Will download {filename}')
                            times.append(t)
                            dirs.append(yearmonth)
                            names.append(filename)
            ftp.cwd('..')
    
    newfile_df = DataFrame({'time': times, 'dir': dirs, 'name': names}).set_index('time')
    newfile_df = newfile_df.sort_index()
    
    if len(newfile_df) > 0:
        # download files that are on the FTP server and not in file_df
        #newfile_df = newfile_df.applymap(lambda x: os.path.basename(x))
        file_df = pd.concat([file_df, newfile_df])
        file_df.to_pickle(f'{ftp_dst_dir}/file_df.pkl')

    ftp.quit()

def shrink(login, url, log, src_path, shrink_dir, utc_offset, keepgpm, reset=False):
    """Shrink the original GPM files to 6N-1N/57W-51W (French Guiana region). Original files are located in 'src_path' and shrunk files in 'shrink_dir'."""
    ftp = ftp_login(url, login, 'NRTPUB/imerg/early', log)

    file_df = pd.read_pickle(f'{src_path}/file_df.pkl')
    mask = pickle.load(open(shrink_dir + '/corr/mask.pkl', 'rb'))
    p_corr_vs_sat_per_month_per_region = pickle.load(open(shrink_dir + '/corr/p_corr_vs_sat_per_month_per_region.pkl', 'rb'))

    x0 = int((-57 - (-180)) / 0.1)
    x1 = int((-51 - (-180)) / 0.1)
    y0 = int(-(-90 - 1) / 0.1) + 1
    y1 = int(-(-90 - 6) / 0.1) + 1

    if reset:
        new_files = file_df.name.values
        DataFrame(data={'date': np.nan, 'p_1d': np.nan, 'p_30m': new_files}).to_pickle(shrink_dir + '/status.p')
    status = pd.read_pickle(shrink_dir + '/status.p')
    new_files = status[status['p_1d'].isnull()]['p_30m'].tolist()
    processed_files = []
    for filename in new_files:
        log.write(f'Processing file {filename}')
        ftpdir = file_df[file_df.name==filename]['dir'].values[0]
        path = f'{ftpdir}/{filename}'
        processed_files.append(f'{src_path}/{filename}')
        try:
            f = h5py.File(f'{src_path}/{filename}', 'r')
            log.write(f'Already downloaded')
        except:
            log.write(f'Downloading')
            with open(f'{src_path}/{filename}', 'wb') as f:
                downloaded = False
                while not downloaded:
                    try:
                        ftp.retrbinary(f'RETR {path}', f.write)
                        downloaded = True
                    except:
                        ftp = ftp_login(url, login, 'NRTPUB/imerg/early', log)
            f = h5py.File(f'{src_path}/{filename}', 'r')
        file_date = file_df.index[file_df.name==filename][0].to_pydatetime() - timedelta(hours=utc_offset)
        data = np.array(f['Grid/precipitationCal'])[x0:x1, y0:y1].transpose()[::-1] / 2 # divide by 2 because in mm/h
        np.clip(data, 0, np.inf, out=data)
        f.close()
        this_date = file_date.date()
        p_1d_filename = 'p_1d_' + str(this_date) + '.npy'
        if p_1d_filename in status.p_1d.tolist():
            this_data = data + np.load(shrink_dir + 'orig_' + p_1d_filename)
            log.write('Added to ' + p_1d_filename)
        else:
            this_data = data
        np.save(shrink_dir + 'orig_' + p_1d_filename[:-4], this_data)
        if pd.isnull(status.loc[status['p_30m'].tolist().index(filename), 'p_1d']):
            status.loc[status['p_30m'].tolist().index(filename), ['date', 'p_1d']] = this_date, p_1d_filename
        else:
            status.loc[len(status)] = this_date, p_1d_filename, filename
        if len(status.date[status.date == this_date]) == 48: # there must be 48 half-hour precipitation estimates for one day to be complete
            corr_p = np.zeros((20, 24))
            resized_data = resize(this_data, 0.4)
            for this_region in ['coast', 'inland', 'regina']:
                #this_mask = resize(mask[this_region], 2.5)
                corr_p += np.interp(resized_data, p_corr_vs_sat_per_month_per_region['trmm_3b42rt']['[2000-03-01 06:00:00+00:00, 2014-01-01 06:00:00+00:00]'][this_date.month][this_region].index.values, p_corr_vs_sat_per_month_per_region['trmm_3b42rt']['[2000-03-01 06:00:00+00:00, 2014-01-01 06:00:00+00:00]'][this_date.month][this_region]['corr'].values) * mask[this_region]
            np.clip(corr_p, 0., np.inf, out=corr_p)
            np.save(shrink_dir + p_1d_filename[:-4], corr_p)
            if not keepgpm:
                for fpath in processed_files:
                    if os.path.exists(fpath):
                        os.remove(fpath)
                        log.write(f'Removed file {os.path.basename(fpath)}')
                processed_files = []
    status = status.sort_values(by=['date'])
    status.index = range(len(status))
    status.to_pickle(shrink_dir + '/status.p')
    ftp.quit()

def make_p_csv(shrink_dir, csv_dir, p_day_nb):
    """Make the precipitation CSV file. It consists of the 2D precipitation covering the past 'p_day_nb' days in the 6N-1N/57W-51W region."""
    status = pd.read_pickle(shrink_dir + '/status.p')
    dates, filenames2 = [], []
    for dirname, dirnames, filenames in os.walk(shrink_dir):
        for filename in filenames:
            if filename.startswith('p_1d_'):
                this_date = date(int(filename[5:9]), int(filename[10:12]), int(filename[13:15]))
                if len(status.date[status.date == this_date]) == 48:
                    dates.append(this_date)
                    filenames2.append(filename)
    tmp = DataFrame({'dates': dates, 'filenames': filenames2})
    tmp = tmp.sort_values(by=['dates'])
    p_dates = list(tmp['dates'].values[-p_day_nb:])
    p_filenames = list(tmp['filenames'].values[-p_day_nb:])
    
    lons = [-57., -51.]
    lats = [1., 6.]
    delta_lon, delta_lat = .1, .1
    col_nb = int((lons[1] - lons[0]) / delta_lon)
    lin_nb = int((lats[1] - lats[0]) / delta_lat)
    
    f = open(csv_dir + 'p_data.csv', 'wt')
    f.write('date')
    header = True
    this_date = p_dates[0]
    for p_filename in [None] + p_filenames:
        if not header:
            f.write('%02d:%02d:%04d' % (this_date.month, this_date.day, this_date.year))
            data_array = np.load(shrink_dir + 'orig_' + p_filename)
        i = 0
        done = False
        while not done:
            if header:
                f.write(',' + str(i))
            else:
                f.write(',' + str(data_array[int((col_nb * lin_nb - 1 - i) / col_nb),  i % col_nb]))
            i += 1
            if i == col_nb * lin_nb:
                done = True
        f.write('\n')
        if not header:
            this_date += timedelta(days=1)
        header = False
    f.close()

def get_pe_ws(shrink_dir, ws_dir, ws_names, reset=False):
    """Get precipitation and potential evapotranspiration over the catchments. The catchments' name are listed in 'ws_names', there masks are located in 'ws_dir/mask'."""
    status = pd.read_pickle(shrink_dir + '/status.p')
    dates_1d = list(set(status.date.tolist()))
    dates_1d.sort()
    dates_1d_complete = []
    for this_date in dates_1d:
        if len(status.date[status.date == this_date]) == 48:
            dates_1d_complete.append(this_date)
    if reset:
        peq = DataFrame({**{'P(' + this_ws + ')': np.nan for this_ws in ws_names}, **{'E(' + this_ws + ')': np.nan for this_ws in ws_names}, **{'Q(' + this_ws + ')': np.nan for this_ws in ws_names}, **{'GR4J(' + this_ws + ')': np.nan for this_ws in ws_names}}, index=dates_1d_complete)
        new_dates = list(set(peq.index.tolist()))
        peq.to_pickle(ws_dir + 'peq.p')
    else:
        peq = pd.read_pickle(ws_dir + '/peq.p')
        new_dates = list(set(peq.index.tolist()) ^ set(dates_1d_complete))
    ws_mask = {}
    for this_ws in ws_names:
        ws_filename = this_ws.lower().replace(' ', '_')
        ws_mask[this_ws] = np.load(ws_dir + '/mask/' + ws_filename + '.npy')
        #ws_mask[this_ws] = resize(np.load(ws_dir + '/mask/' + ws_filename + '.npy'), 2.5)
    etp = [i / 30. for i in [101, 110, 126, 125, 108, 105, 120, 145, 160, 160, 155, 150]]
    for this_date in new_dates:
        this_p = np.load(shrink_dir + '/p_1d_' + str(this_date) + '.npy')
        for this_ws in ws_names:
            ws_filename = this_ws.lower().replace(' ', '_')
            ws_p = np.sum(this_p * ws_mask[this_ws])
            peq.loc[this_date, 'P(' + this_ws + ')'] = ws_p
            peq.loc[this_date, 'E(' + this_ws + ')'] = etp[this_date.month - 1]
    peq.to_pickle(ws_dir + '/peq.p')

def get_q_ws(ws_dir, ws_names, gr4j_x):
    """Get the simulated streamflow for each catchment. 'gr4j_x' consists of the GR4J rainfall-runoff model parameters."""
    peq = pd.read_pickle(ws_dir + '/peq.p')
    gr4j_s = {}
    for this_ws in ws_names:
        _, gr4j_s[this_ws] = gr4j(gr4j_x[this_ws], [0., 0.], None)
    for this_date in peq.index.tolist():
        for this_ws in ws_names:
            if pd.isnull(peq.loc[this_date, 'Q(' + this_ws + ')']):
                this_q, gr4j_s[this_ws] = gr4j(gr4j_x[this_ws], [peq.loc[this_date, 'P(' + this_ws + ')'], peq.loc[this_date, 'E(' + this_ws + ')']], gr4j_s[this_ws])
                peq.loc[this_date, 'GR4J(' + this_ws + ')'] = str(gr4j_s[this_ws])
                peq.loc[this_date, 'Q(' + this_ws + ')'] = this_q
            else:
                gr4j_s[this_ws] = [float(this_s) for this_s in peq['GR4J(' + this_ws + ')'][this_date].replace('[', '').replace(']', '').replace(' ', '').split(',')]
    peq.to_pickle(ws_dir + '/peq.p')

def make_q_json(ws_dir, csv_dir, q_day_nb, ws_names):
    peq = pd.read_pickle(ws_dir + '/peq.p')
    for this_ws in ws_names:
        ws_filename = this_ws.lower().replace(' ', '_')
        f = open(csv_dir + 'q_' + ws_filename + '.json', 'wt')
        #f.write(str([[i, peq.loc[this_date, 'Q(' + this_ws + ')']] for i, this_date in enumerate(peq.index.tolist()[-q_day_nb:])]))
        f.write(str([peq.loc[this_date, 'Q(' + this_ws + ')'] for this_date in peq.index.tolist()[-q_day_nb:]]))
        f.close()
    f = open(csv_dir + 'lastDate.csv', 'wt')
    f.write('lastDate\n')
    f.write(str(peq.index.tolist()[-1]))
    f.close()

def make_download_files(shrink_dir, ws_dir, csv_dir, ws_names, from_date):
    """Make the files to be downloaded: catchment rainfall and streamflow (daily, monthly, yearly), 2D rainfall."""
    peq = pd.read_pickle(ws_dir + '/peq.p')
    cols = []
    for this_peq in ['P', 'Q']:
        for this_ws in ws_names:
            cols.append(this_peq + '(' + this_ws + ')')
    peq.index = pd.date_range(peq.index[0], peq.index[-1])
    peq.index.names = ['Dates']
    peq = peq.loc[from_date:, cols]
    peq.applymap(lambda x: '%.1f' % x).to_csv(csv_dir + '/pq_1d.csv')
    peq.resample('M').sum().loc[:, cols].applymap(lambda x: '%.1f' % x).to_csv(csv_dir + '/pq_1m.csv')
    peq.resample('A').sum().loc[:, cols].applymap(lambda x: '%.1f' % x).to_csv(csv_dir + '/pq_1y.csv')

    status = pd.read_pickle(shrink_dir + '/status.p')
    driver = gdal.GetDriverByName('GTiff')
    for this_file in status[status.date >= datetime.strptime(from_date, "%Y-%m-%d").date()].loc[:, 'p_1d'].values:
        tif_file = 'p2d_1d/' + this_file[:-4] + '.tif'
        if not os.path.exists(tif_file):
            this_data = np.load(shrink_dir + this_file)
            ws_ds = driver.Create(tif_file, this_data.shape[1], this_data.shape[0], 1, gdal.GDT_Float32)
            ws_ds.SetGeoTransform((-57., 0.1, 0.0, 6., 0.0, -0.1))
            ws_ds.SetProjection('GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]]')
            ws_ds.GetRasterBand(1).WriteArray(this_data)
            ws_ds = None
    with ZipFile('p2d_1d.zip', 'a') as zf:
        for this_file in list(set(status.loc[:, 'p_1d'].values)):
            this_member = 'p2d_1d/' + this_file[:-4] + '.tif'
            if this_member not in zf.namelist():
                zf.write(this_member)

@click.command()
@click.option('--reset', is_flag=True, help='Reset the processing chain.')
@click.option('--logfile', default='tmp/log.txt', help='Path to log file.')
@click.option('--printout', is_flag=True, help='Print log to stdout.')
@click.option('--keepgpm', is_flag=True, help='Keep GPM downloaded files.')
@click.option('--login', default='david.brochart@free.fr', help='Email address to log into FTP server.')
@click.option('--fromdate', default='2014-03-12', help='Date from which to process precipitation data.')
def main(reset, logfile, printout, keepgpm, login, fromdate):
    if not os.path.exists('../data/grid.json'):
        grid.make()
    url = 'jsimpson.pps.eosdis.nasa.gov'
    # create tmp directory, in which there is log.txt where debugging information is written
    os.makedirs('tmp', exist_ok=True)
    os.makedirs('gpm_data', exist_ok=True)
    os.makedirs('gpm_csv', exist_ok=True)
    os.makedirs('gpm_ws', exist_ok=True)
    os.makedirs('p2d_1d', exist_ok=True)
    log = Log(logfile, printout)
    # create (if doesn't exist) or load "file_df.pkl", this file keeps track of GPM files that have been downloaded
    ftp_dst_dir = 'gpm_data/'
    utc_offset = 3 # 0 if UTC, 3 if French Guiana
    p_day_nb = 30
    q_day_nb = 1024
    shrink_dir = 'gpm_shrink/'
    csv_dir = 'gpm_csv/'
    ws_dir = 'gpm_ws/'
    gr4j_x = {
            'Saut Sabbat':      [2919.540841224371, -0.86168151155171913, 84.70999803176737, 3.4981162284065435],
            'Saut Bief':        [3238.6514534172547, 0.33831940503407765, 74.518651431825504, 2.511460578426322],
            'Saut Athanase':    [4438.1659870265757, 0.73540066060669718, 101.61011800978221, 2.4594558605711621],
            'Saut Maripa':      [2581.211011708363, -0.32108302526110438, 115.03812899301911, 3.3598618612404842],
            'Maripasoula':      [3003.1886179611165, -2.2929565882464029, 116.05308733707037, 4.4106266442720621],
            'Langa Tabiki':     [2677.4118727879959, -1.7274632708443527, 136.1150157663804, 4.6408104323814863]
            }
    ws_names = list(gr4j_x.keys())
    while True: # program main loop
        get_new_files(login, url, log, ftp_dst_dir, fromdate, reset)
        shrink(login, url, log, ftp_dst_dir, shrink_dir, utc_offset, keepgpm, reset)
        make_p_csv(shrink_dir, csv_dir, p_day_nb)
        get_pe_ws(shrink_dir, ws_dir, ws_names, reset)
        get_q_ws(ws_dir, ws_names, gr4j_x)
        make_q_json(ws_dir, csv_dir, q_day_nb, ws_names)
        make_download_files(shrink_dir, ws_dir, csv_dir, ws_names, fromdate)
        reset = False
        log.write('Now going to sleep...')
        sleep(1800) # going to sleep for half an hour

if __name__ == '__main__':
    main()
