from mpi4py import MPI
import sys

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    # Need at least 3 processes (1 master, 1 crawler, 1 indexer)
    if size < 3:
        print("Error: Need at least 3 processes")
        return
    
    # Process 0 is the master
    if rank == 0:
        from master_node import master_process
        master_process()
    # Processes 1 to size-2 are crawlers
    elif rank < size - 1:
        from crawler_node import crawler_process
        crawler_process()
    # Process size-1 is the indexer
    else:
        from indexer_node import indexer_process
        indexer_process()

if __name__ == "__main__":
    main()