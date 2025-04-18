# Distributed-Web-Crawling-Indexing-System

# first download this (both files): 
https://www.microsoft.com/en-us/download/details.aspx?id=57467
, setup both files

# Open the folder where you've cloned the project then open the terminal
# run this command to install all the libraries needed
pip install mpi4py requests beautifulsoup4 whoosh
# run this command to run the system
mpiexec -n 3 python main.py
