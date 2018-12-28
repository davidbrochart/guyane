import click
import pickle
import numpy as np
from datetime import datetime, timedelta
from time import sleep
import os
import pandas as pd
from pandas import DataFrame
from zipfile import ZipFile
from osgeo import gdal
from gr4j import gr4j
import h5py
import subprocess
import shutil
from skimage.transform import resize
import grid

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
            self.f.write(f'{string}\n')
            self.f.flush()

def get_gpm(url, login, log, src_path, shrink_dir, keepgpm, fromdate, todate, reset=False):
    """Get the original GPM files for 1N-6N/57W-51W (French Guiana region)."""
    if reset:
        status = DataFrame(data={'datetime': [], 'date': [], 'p_1d': []}).set_index('datetime')
        this_datetime = datetime(*(int(i) for i in fromdate.split('-')))
    else:
        status = pd.read_pickle(shrink_dir + '/status.p')
        this_datetime = status.index[-1] + timedelta(minutes=30) # next GPM

    to_date_dt = datetime.strptime(todate, "%Y-%m-%d").date()
    mask = pickle.load(open(shrink_dir + '/corr/mask.pkl', 'rb'))
    p_corr_vs_sat_per_month_per_region = pickle.load(open(shrink_dir + '/corr/p_corr_vs_sat_per_month_per_region.pkl', 'rb'))

    x0 = int((-57 - (-180)) / 0.1)
    x1 = int((-51 - (-180)) / 0.1)
    y0 = int(-(-90 - 1) / 0.1)
    y1 = int(-(-90 - 6) / 0.1)

    if not ((this_datetime.hour == 3) and (this_datetime.minute == 0)):
        this_datetime = this_datetime + timedelta(days=1)
        this_datetime = this_datetime.replace(hour=3, minute=0)

    day_complete = False
    skip = False
    while True:
        datetimes = [this_datetime + timedelta(minutes=30*i) for i in range(48)]
        urls, filenames = [], []
        for t in datetimes:
            year = t.year
            month = str(t.month).zfill(2)
            day = str(t.day).zfill(2)
            hour = str(t.hour).zfill(2)
            min0 = str(t.minute).zfill(2)
            min1 = t.minute + 29
            minutes = str(t.hour*60+t.minute).zfill(4)
            filename = f'3B-HHR-E.MS.MRG.3IMERG.{year}{month}{day}-S{hour}{min0}00-E{hour}{min1}59.{minutes}.V05B.RT-H5'
            urls.append(f"ftp://{url}/NRTPUB/imerg/early/{year}{month}/{filename}")
            filenames.append(filename)
        with open('tmp/gpm_list.txt', 'w') as f:
            f.write('\n'.join(urls))
        try:
            subprocess.check_call(f'aria2c -x 16 -i tmp/gpm_list.txt -d {src_path} --ftp-user={login} --ftp-passwd={login} --continue=true'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            skip = False
        except:
            if this_datetime.date() > to_date_dt:
                return day_complete
            else:
                skip = True

        if not skip:
            day_complete = True
            for filename, dt in zip(filenames, datetimes):
                try:
                    f = h5py.File(f'{src_path}/{filename}', 'r')
                    data = np.array(f['Grid/precipitationCal'])[x0:x1, y0:y1].transpose()[::-1] / 2 # divide by 2 because in mm/h
                    data = np.clip(data, 0, np.inf)
                    f.close()
                except:
                    data = np.zeros((y1-y0, x1-x0), dtype=np.float32)
                this_date = (dt - timedelta(hours=3)).date() # French Guiana is UTC-3
                p_1d_filename = f'p_1d_{this_date}.npy'
                if p_1d_filename in status.p_1d.tolist():
                    this_data = data + np.load(shrink_dir + 'orig_' + p_1d_filename)
                else:
                    this_data = data
                    log.write('Created ' + p_1d_filename)
                log.write('Added ' + filename)
                np.save(shrink_dir + 'orig_' + p_1d_filename[:-4], this_data)
                status.loc[dt] = this_date, p_1d_filename
                status.to_pickle(f'{shrink_dir}/status.p')
            this_datetime = this_datetime + timedelta(days=1)

            corr_p = np.zeros((20, 24))
            vmax = np.max(this_data)
            resized_data = resize(this_data/vmax, (20, 24), anti_aliasing=False, mode='constant') * vmax
            for this_region in ['coast', 'inland', 'regina']:
                corr_p += np.interp(resized_data, p_corr_vs_sat_per_month_per_region['trmm_3b42rt']['[2000-03-01 06:00:00+00:00, 2014-01-01 06:00:00+00:00]'][this_date.month][this_region].index.values, p_corr_vs_sat_per_month_per_region['trmm_3b42rt']['[2000-03-01 06:00:00+00:00, 2014-01-01 06:00:00+00:00]'][this_date.month][this_region]['corr'].values) * mask[this_region]
            np.clip(corr_p, 0., np.inf, out=corr_p)
            np.save(shrink_dir + p_1d_filename[:-4], corr_p)
            if not keepgpm:
                shutil.rmtree(src_path, ignore_errors=True)
                os.makedirs(src_path, exist_ok=True)

def make_p_csv(shrink_dir, csv_dir, p_day_nb):
    """Make the precipitation CSV file. It consists of the 2D precipitation covering the past 'p_day_nb' days in the 6N-1N/57W-51W region."""
    status = pd.read_pickle(shrink_dir + '/status.p')
    df = status.drop_duplicates(['date', 'p_1d'])
    dates = df.date.tolist()
    filenames = df.p_1d.tolist()
    p_dates = dates[-p_day_nb:]
    p_filenames = filenames[-p_day_nb:]

    lons = [-57., -51.]
    lats = [1., 6.]
    delta_lon, delta_lat = .1, .1
    col_nb = int((lons[1] - lons[0]) / delta_lon)
    row_nb = int((lats[1] - lats[0]) / delta_lat)

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
                f.write(',' + str(data_array[int((col_nb * row_nb - 1 - i) / col_nb),  i % col_nb]))
            i += 1
            if i == col_nb * row_nb:
                done = True
        f.write('\n')
        if not header:
            this_date += timedelta(days=1)
        header = False
    f.close()

def get_pe_ws(shrink_dir, ws_dir, ws_names, reset=False):
    """Get precipitation and potential evapotranspiration over the catchments. The catchments' name are listed in 'ws_names', there masks are located in 'ws_dir/mask'."""
    status = pd.read_pickle(shrink_dir + '/status.p')
    df = status.drop_duplicates(['date'])
    dates = df.date.tolist()
    if len(status.loc[status.date==dates[-1]]) != 48: # last date might not be complete
        dates.pop()
    if reset:
        peq = DataFrame({**{'P(' + this_ws + ')': np.nan for this_ws in ws_names}, **{'E(' + this_ws + ')': np.nan for this_ws in ws_names}, **{'Q(' + this_ws + ')': np.nan for this_ws in ws_names}, **{'GR4J(' + this_ws + ')': np.nan for this_ws in ws_names}}, index=dates)
        new_dates = peq.index.tolist()
        peq.to_pickle(f'{ws_dir}peq.p')
    else:
        peq = pd.read_pickle(f'{ws_dir}/peq.p')
        new_dates = list(set(peq.index.tolist()) ^ set(dates))
    ws_mask = {}
    for this_ws in ws_names:
        ws_filename = this_ws.lower().replace(' ', '_')
        ws_mask[this_ws] = np.load(ws_dir + '/mask/' + ws_filename + '.npy')
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
        if this_ws in list(gr4j_x.keys()):
            _, gr4j_s[this_ws] = gr4j(gr4j_x[this_ws], [0., 0.], None)
    for this_date in peq.index.tolist():
        for this_ws in ws_names:
            if this_ws in list(gr4j_x.keys()):
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
    from_date_dt = datetime.strptime(from_date, "%Y-%m-%d").date()
    peq = pd.read_pickle(ws_dir + '/peq.p')
    cols = []
    for this_peq in ['P', 'Q']:
        for this_ws in ws_names:
            cols.append(this_peq + '(' + this_ws + ')')
    peq.index = pd.date_range(peq.index[0], peq.index[-1])
    peq.index.names = ['Dates']
    peq = peq.loc[from_date_dt:, cols]
    peq.applymap(lambda x: '%.1f' % x).to_csv(csv_dir + '/pq_1d.csv')
    peq.resample('M').sum().loc[:, cols].applymap(lambda x: '%.1f' % x).to_csv(csv_dir + '/pq_1m.csv')
    peq.resample('A').sum().loc[:, cols].applymap(lambda x: '%.1f' % x).to_csv(csv_dir + '/pq_1y.csv')

    status = pd.read_pickle(shrink_dir + '/status.p')
    driver = gdal.GetDriverByName('GTiff')
    for this_file in status[status.date >= from_date_dt].loc[:, 'p_1d'].values:
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
@click.option('--todate', default='2018-12-10', help='Date until which to process precipitation data without waiting (but keep on processing after this date anyway).')
def main(reset, logfile, printout, keepgpm, login, fromdate, todate):
    if not os.path.exists('../data/grid.json'):
        grid.make()
    url = 'jsimpson.pps.eosdis.nasa.gov'
    # create tmp directory, in which there is log.txt where debugging information is written
    os.makedirs('tmp', exist_ok=True)
    shutil.rmtree('gpm_data', ignore_errors=True)
    os.makedirs('gpm_data', exist_ok=True)
    os.makedirs('gpm_csv', exist_ok=True)
    os.makedirs('gpm_ws', exist_ok=True)
    os.makedirs('p2d_1d', exist_ok=True)
    log = Log(logfile, printout)
    # create (if doesn't exist) or load "file_df.pkl", this file keeps track of GPM files that have been downloaded
    ftp_dst_dir = 'gpm_data/'
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
    ws_names = list(gr4j_x.keys()) + ['Tapanahony']
    while True: # program main loop
        if get_gpm(url, login, log, ftp_dst_dir, shrink_dir, keepgpm, fromdate, todate, reset):
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
