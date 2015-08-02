import requests
import concurrent
import shutil
import lxml
from concurrent.futures import ThreadPoolExecutor
import os
from pyquery import PyQuery as pq

data_dir = "data/"  # Cheat with globals

def get_months(year_dom):
	skip_months = ["month_01", "month_02"]

	months = []
	for a in year_dom("a"):
		month_text = a.text.strip()
		if month_text.strip("/") in skip_months:
			continue
		if month_text.startswith("month_"):
			months.append(month_text)
	return months

def get_dom(requests_response):
	content = requests_response.content
	return pq(content)

def get_days(root_url, month_path):
	days = []
	resp = requests.get(root_url + month_path)
	month_listing_dom = get_dom(resp)
	for a in month_listing_dom("a"):
		if a.text.strip().startswith("day"):
			days.append(a.text.strip())
	return days
		
def get_game_paths(*paths):	
	path = os.path.join(*paths)
	resp = requests.get(path)
	day_listing_dom = get_dom(resp)
	game_paths = []
	for a in day_listing_dom("a"):
		if a.text.strip().startswith("gid_"):
			game_paths.append(a.text.strip())
	return game_paths
	

def get_inning_data(*paths):
	path = os.path.join(*paths)
	resp = requests.get(path)
	game_listing_dom = get_dom(resp)
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
	#shutil.rmtree(path, ignore_errors=True)
	try:
		os.makedirs(path)
	except OSError as e:
		if e.errno == 17:  # File exists
			pass
		else:
			raise

def save_inning_data(inning_dir, url, data):
	inning_file = os.path.join(inning_dir, "inning_all.xml")
	print("Writing data found at " + url + " file at " + inning_file)
	save_data(inning_file, data)

def save_data(path, data):
	if not path.startswith("data/"):
		import pdb;pdb.set_trace()
	with open(path, "w+") as fd:
		try:
			pp_payload = lxml.etree.fromstring(data)
			pp_string = lxml.etree.tostring(pp_payload, pretty_print = True)
			fd.write(str(pp_string))
		except lxml.etree.XMLSyntaxError as e:
			print("Failed to parse xml found at " + path + " . Writing raw payload without formating")
			fd.write(str(data))

def get_players_data(root_path, game_path):
	players_path = os.path.join(game_path, "players.xml")
	url = os.path.join(root_path, game_path, "players.xml")
	resp = requests.get(url)
	return url, players_path, resp.content

def save_game(root_path, *paths):
	mkdir_p(os.path.join(data_dir, *paths))
	players_url, players_path, players_data = get_players_data(root_path, os.path.join(*paths))
	target = os.path.join(data_dir, players_path)
	print("Writing data found at " + players_url + " to " + target)
	save_data(target, players_data)

	for url, data in get_inning_data(root_path, *paths):
		full_game_path = os.path.join(*paths)
		inning_dir = os.path.join(data_dir, full_game_path, "inning")
		mkdir_p(inning_dir)
		save_inning_data(inning_dir, url, data)

def make_task(fn, *args, **kwargs):
	return lambda: fn(*args, **kwargs)

def save_month(root_url, month_path):
	day_paths = get_days(root_url, month_path)
	for day_path in day_paths:
		game_paths = get_game_paths(root_url, month_path, day_path)
		if not game_paths:
			print("Skipping " + root_url + month_path + day_path)
			continue
		for game_path in game_paths:
			task = make_task(save_game, root_url, month_path, day_path, game_path)
			yield task

def main():

	proto = "http"
	base_path = "://gd2.mlb.com/components/game/mlb/year_"
	year = "2014"

	num_workers = 20 * 2

	root_url = proto + base_path + year + "/"
	resp = requests.get(root_url)
	year_listing = resp.content

	year_dom = pq(year_listing)
	month_paths = get_months(year_dom)
			


	task_progress = 1
	with ThreadPoolExecutor(max_workers=num_workers) as executor:
		futures = []
		for month_path in month_paths:
			futures.append(executor.submit(save_month, root_url, month_path))

		for future in concurrent.futures.as_completed(futures):
			for task in future.result():
				executor.submit(task)

if __name__ == '__main__':
	main()
