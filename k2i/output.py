
import plotly.plotly as py
from plotly.offline import download_plotlyjs, init_notebook_mode, plot
import plotly.graph_objs as go
import pandas as pd
import glob
import numpy as np

# header values for .log files
headers = [
	"job-id", 
	"cluster", 
	"partition",
	"account",
	"group",
	"user",
	"submit-time",
	"eligible-time",
	"start-time",
	"end-time",
	"elapsed-time",
	"exit code",
	"num-nodes",
	"num-cpus",
	"node-list",
	"job-name"
]

def sl_to_df(fl):
	"""
	converts slurm .log file to a dataframe using pandas
	"""

	# convert the .log to a pandas table
	df = pd.read_table(fl, sep = "|", names = headers)

	# change everything to uniform data type
	df['job-id'] = df['job-id'].astype(str)

	# filter out zero cpus used for figuring out factors
	df = df[(df['num-cpus'] != 0)]

	return df


def find_gcf(a, b):
	"""
	finds the greatest common factor between two numbers
	O(logn)
	"""
	if (b == 0):
		return a
	return find_gcf(b, a % b)


# have a refactor boolean as an argument
def findfactor(path, refactor=False):
	"""
	finds the gcf for each .log file
	"""
	# read all .csv's
	rd_fls = sorted(glob.glob(path + "/*.log"))
	n = len(rd_fls)

	# initialize new dataframe 
	out = pd.DataFrame(columns = ["file", "len", "avg", "factor"])

	# iterate through all of the .log files
	for i in range(n):
		avg = -1
		df = sl_to_df(rd_fls[i])
		array = df['num-cpus'].tolist()		# all cpus for that .log file
		m = len(array)
		gcf = -1

		# ignore empty .log files
		if (m > 0):
			# iterate through each row of a specific .log file
			for j in range(m):
				# find all .log files with .5 multiples present
				if ((array[j] - .5) % 1 == 0):
					gcf = 0.5
			# find the gcf if no .5 values in log file
			if (gcf == -1):
				# reduce is essentailly the same as 'foldl'
				gcf = reduce((lambda x, y: find_gcf(x, y)), array)

			# find average number of cpus per .log file			
			avg = sum(array) / m
			# out.loc[i] = [str(rd_fls[i][10:]), m, avg, gcf]

			# apply refactoring where appropriate
			if (refactor):
				if (gcf == 0.5):
					df.loc[:, 'num-cpus'] *= 2
				if (gcf >= 2.0):
					df.loc[:, 'num-cpus'] *= 0.5
				ref_array = df['num-cpus'].tolist()		# refactored array
				ref_m = len(ref_array)
				avg = sum(ref_array) / ref_m

			# write values to new output table
			out.loc[i] = [str(rd_fls[i][10:]), m, avg, gcf]

	return out

# print findfactor("data/nots").to_csv("slurm-avgs.csv", index = False)


def plot_df(df):
	"""
	uses plotly to plot the data
	"""
	array = df['factor'].tolist()
	n = len(array)

	red = 'rgb(222,0,0)'
	grn = 'rgb(0,222,0)'
	blu = 'rgb(0,0,222)'
	nul = 'rgb(0,0,0)'

	# colors points based on factor
	clr = list()
	for i in range(n):
		factor = array[i]
		if factor == 1:
			clr.append(red)
		elif factor > 1:	# factor of 2+
			clr.append(grn)
		else:				# factor of .5
			clr.append(blu)

	# general layout
	layout = go.Layout(
		# title = 'Average Number of CPUs per Slurm File',
		title = 'Average Number of CPUs per Slurm File (Refactored)',
		yaxis = dict(
			title = 'Average Number of CPUs Used', 
			range = [0, 130]
		),
		showlegend = False
	)

	trace = go.Scatter(
		x = df['file'],
		y = df['avg'],
		mode = 'markers',
		marker = dict(color = clr)
	)

	data = [trace]
	fig = go.Figure(data = data, layout = layout)

	return plot(fig)


df = findfactor("data/nots")
ref_df = findfactor("data/nots", True)
# print ref_df
# print plot_df(df)
# print plot_df(ref_df)

"""
for any ouputs, uncomment print statements on:
lines 107, 159, 160, and/or 161
"""
