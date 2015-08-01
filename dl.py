import requests
import shutil
from lxml import etree
import os
from pyquery import PyQuery as pq

proto = "http"
base_path = "://gd2.mlb.com/components/game/mlb/year_"
year = "2014"
skip_months = ["month_01", "month_02"]

data_dir = "data/"

root_url = proto + base_path + year + "/"
resp = requests.get(root_url)
year_listing = resp.content

year_dom = pq(year_listing)

def get_months(year_dom):
	months = []
	for a in year_dom("a"):
		month_text = a.text.strip()
		if month_text.strip("/") in skip_months:
			continue
		if month_text.startswith("month_"):
			months.append(month_text)
	return months

def get_days(root_url, month_path):
	days = []
	resp = requests.get(root_url + month_path)
	month_listing = resp.content
	month_listing_dom = pq(month_listing)
	for a in month_listing_dom("a"):
		if a.text.strip().startswith("day"):
			days.append(a.text.strip())
	return days
		
def get_game_paths(*paths):	
	path = "".join(paths)
	resp = requests.get(path)
	day_listing = resp.content
	day_listing_dom = pq(day_listing)
	game_paths = []
	for a in day_listing_dom("a"):
		if a.text.strip().startswith("gid_"):
			game_paths.append(a.text.strip())
	return game_paths
	

def get_inning_data(*paths):
	path = "".join(paths)
	resp = requests.get(path)
	game_listing = resp.content
	game_listing_dom = pq(game_listing)
	inning_data = []
	for a in game_listing_dom("a"):
		inning_path = a.text.strip()
		if inning_path != "inning/":
			continue
		data_path = os.path.join(path, inning_path, "inning_all.xml")
		i_resp = requests.get(data_path)
		inning_data.append((data_path, i_resp.content))
	return inning_data


def mkdir_p(path):
	shutil.rmtree(path, ignore_errors=True)
	os.makedirs(path)

month_paths = get_months(year_dom)
inning_data = {}
for month_path in month_paths:
	day_paths = get_days(root_url, month_path)
	for day_path in day_paths:
		game_paths = get_game_paths(root_url, month_path, day_path)
		if not game_paths:
			print "Skipping " + root_url + month_path + day_path
			continue
		for game_path in game_paths:
			for url, data in get_inning_data(root_url, month_path, day_path, game_path):
				inning_dir = os.path.join(data_dir, month_path, day_path, game_path, "inning")
				inning_file = os.path.join(inning_dir, "inning_all.xml")
				mkdir_p(inning_dir)
				with open(inning_file, "w+") as fd:
					print "Writing data found at " + url + " file at " + inning_file
					pp_payload = etree.fromstring(data)
					fd.write(etree.tostring(pp_payload, pretty_print = True))
