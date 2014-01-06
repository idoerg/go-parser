
import networkx as nx
import go_obo_parser as gop

def read_go_in_dag(obo_file):
    for go_node in gop.parseGOOBO(obo_file):
        
