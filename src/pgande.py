"""PG&E DLP data access"""
import os
import pycurl
from io import BytesIO, StringIO, IOBase
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from zipfile import ZipFile

dlpurl = "https://www.pge.com/pge_global/forms/mads/profiles"
cachedir = "__dlpcache__"

if not os.path.exists(cachedir):
	os.mkdir(cachedir)

def get_remote_file(url,out):
	"""Stream a file from the PG&E data archive"""
	c = pycurl.Curl()
	c.setopt(c.URL, url)
	c.setopt(c.WRITEDATA, out)
	c.perform()
	c.close()

def get_load_archive(year,cache=True,refresh=True):
	"""Copy a DLP archive for a previous year to the cache"""
	zipfile = f"{cachedir}/{year}dlp.zip"
	with open(zipfile,"wb") as zipfh:
		get_remote_file(f"{dlpurl}/archive/{year}dlp.zip",zipfh)
	with ZipFile(zipfile, 'r') as zipObj:
		files = zipObj.namelist()
		for file in files:
			if file.endswith('.dlp'):
				zipObj.extract(file, f"{cachedir}/{year}dlp")

def get_load_profile(date,cache=True,refresh=False):
	"""Copy a DLP for a particular date to the cache and return a dataframe"""
	if date.year < datetime.now().year:
		get_load_archive(date.year)

	datename = date.strftime('%Y%m%d')
	if not os.path.exists(f"{cachedir}/{date.year}dlp"):
		os.mkdir(f"{cachedir}/{date.year}dlp")
	csvname = f"{cachedir}/{date.year}dlp/{datename}.dlp"
	if not cache or not os.path.exists(csvname) or refresh:
		with open(csvname,"wb") as csvfh:
			get_remote_file(f'{dlpurl}/{datename}.dlp',csvfh)

	df = pd.read_csv(csvname).dropna(how='all').transpose()
	df.columns = list(np.array(df[1:2])[0])
	assert(datename == df.index[0])
	df.drop([datename,'Profile','Method'],inplace=True)
	def get_time(date,time):
		t = time.split(':')
		t = (24+int(t[0]))*60 + int(t[1]) - 30
		y = int(date[0:4])
		m = int(date[4:6])
		d = int(date[6:8])
		H = int(t/60) % 24
		M = t % 60
		return datetime(y,m,d,H,M,0)
	df['datetime'] = list(map(lambda t: datetime.strptime(datename+" "+t,"%Y%m%d %H:%S"),df.index))
	df.set_index('datetime',inplace=True)

	return df

def daterange(start_date, end_date):
	"""Obtain a date range"""
	for n in range(int ((end_date - start_date).days+1)):
		yield start_date + timedelta(n)

def get_loads(start,stop,date_format='%m/%d/%y',show_progress=False):
	"""Obtain the loads for a date range as a dataframe"""
	if type(start) is str:
		start = datetime.strptime(start,date_format)
	if type(stop) is str:
		stop = datetime.strptime(stop,date_format)
	blocks = []
	for date in daterange(start,stop):
		if show_progress:
			print(f"Processing {date}...",flush=True)
		try:
			blocks.append(get_load_profile(date))
		except Exception as err:
			print(f"ERROR: get_load_profile(date={date}): {err}")
	return pd.concat(blocks)

if __name__ == '__main__':
	get_load_profile(datetime(2019,3,1,0,0,0))
	data = get_loads('3/1/20','3/14/20')
	data.to_csv('test_result.csv')
