
from collections import defaultdict
import go_obo_parser as gop

def print_cafa_go_files(obo_filename):
    for go_term in gop.parseGOOBO(obo_filename):
        if go_term['namespace'] == 'molecular_function':
            molfunc.write("%s\n" % go_term['id'])
        elif go_term['namespace'] == 'biological_process':
            bioproc.write("%s\n" % go_term['id'])
        elif go_term['namespace'] == 'cellular_component':
            cellcomp.write("%s\n" % go_term['id'])
        go_ancestors.write("%s\t" % go_term['id'])
        
def build_go_dags(obo_filename):
    mfo_dag = defaultdict(list) # a directed graph, implemented as a dict
    bpo_dag = defaultdict(list) # a directed graph, implemented as a dict
    cco_dag = defaultdict(list) # a directed graph, implemented as a dict

    for go_term in gop.parseGOOBO(obo_filename):
        if go_term.has_key('is_a'):
            go_id = go_term['id'][0]
            for parent in go_term['is_a']:
                if go_term['namespace'] == ['biological_process']:
                    bpo_dag[go_id].append(parent['id'])
                elif go_term['namespace'] == ['molecular_function']:
                    mfo_dag[go_id].append(parent['id'])
                elif go_term['namespace'] == ['cellular_component']:
                    cco_dag[go_id].append(parent['id'])
    return mfo_dag, bpo_dag, cco_dag

def get_ancestors(term, go_dag,ancestors=[]):
#    print "in=", ancestors
    if ancestors == []:
        ancestors = [term]
    if go_dag[term] != []:
        for parent in go_dag[term]:
            ancestors.append(parent)
            z = get_ancestors(parent,go_dag,ancestors)
    else:
        pass
    #    print "out=", ancestors
    return ancestors

    
        
        
