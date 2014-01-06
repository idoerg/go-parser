import UserDict
import MySQLdb
class GOA_ga(UserDict.UserDict):
    def __init__(self):
        self.data = {}
    def godb_open(self):
        self.dbcon = MySQLdb.connect(host="mysql.ebi.ac.uk", user="go_select",
                   passwd="amigo", db="go_latest", port=4085)
        self.dbcursor = self.dbcon.cursor()
    def godb_close(self):
        self.dbcursor.close()
        

    def read_gene_assoc(self,inpath):
        for inline in file(inpath):
            if inline[0] == '!': continue
            try:
                db, db_object_id, db_object_symbol, qualifier, go_id, \
                db_reference, evidence, withit, aspect, \
                db_object_name, synonym, db_object_type, \
                taxon_id, date, assigned_by, \
                annotation_extension, gene_product_form_id = inline.rstrip('\n').split('\t')
            except ValueError:
                print inline.rstrip('\n').split('\t')
                print len(inline.rstrip('\n').split('\t'))
                raise
            key = db+":"+db_object_id
            self.data.setdefault(key,[]).append({'db':db,'db_object_id':db_object_id,
                                            'db_object_symbol': db_object_symbol,
                                            'qualifier': qualifier,
                                            'go_id': go_id,
                                            'db_reference': db_reference,
                                            'evidence': evidence,
                                            'with': withit,
                                            'aspect': aspect,
                                            'db_object_name': db_object_name,
                                            'synonym': synonym,
                                            'db_object_type': db_object_type,
                                            'taxon_id': taxon_id,
                                            'date': date,
                                            'assigned_by': assigned_by,
                                            'annotation_extension': annotation_extension,
                                            'gene_product_form_id': gene_product_form_id

})


find_parents_go = """
SELECT p.acc
FROM
 graph_path 
  INNER JOIN
 term AS t ON (t.id = graph_path.term2_id)
  INNER JOIN
 term AS p ON (p.id = graph_path.term1_id)
WHERE t.acc = '%s'
"""
find_children_go = """
SELECT child.acc
FROM term AS parent,
     term2term,
     term AS child
WHERE
     parent.acc = '%s' AND
     parent.id = term2term.term1_id AND
     child.id  = term2term.term2_id

"""
        
find_descendants_go = """
SELECT
  rchild.acc
FROM 
  term AS rchild, term AS ancestor, graph_path
WHERE
  graph_path.term2_id = rchild.id and 
  graph_path.term1_id = ancestor.id and 
  ancestor.acc = '%s'

"""
def find_go_descendants(goa_ga, parent_go_id):
    go_children = {}
    found_genes = {}
    # query GO database for all the child terms of the parent_go_id
    goa_ga.dbcursor.execute(find_descendants_go % parent_go_id)
    rpl = goa_ga.dbcursor.fetchall()
    print len(rpl)
    print rpl[:5]
    # Flatten the list. Put it in dictionary keys for faster searching.
    for i in rpl:
        go_children[i[0]] = None
    for gene_id in goa_ga.keys():
        for goa_rec in goa_ga[gene_id]:
            if goa_rec['go_id'] in go_children.keys():
                found_genes.setdefault(gene_id,[]).append(goa_rec)
    return found_genes

def get_goa_histories(file_list):
    # returns a list of goa_dict dictionaries. It is up to the user to make sure the list in
    # chronological order
    all_goa = []
    for i,filename in enumerate(file_list):
        all_goa.append(read_gene_assoc(filename))
    return all_goa

def check_goa_histories(all_goa):
    exp_gene_id_set = set([])
    nonexp_gene_id_set = set([])
    set_list = []
    # get all gene ids
    for goa_dict in all_goa:
        for gene_id in goa_dict:
            for go_rec in goa_dict[gene_id]:
                if go_rec['aspect'] != 'F': continue
                if go_rec['evidence'] != 'EXP' and \
                   go_rec['evidence'] != 'TAS':
                    nonexp_gene_id_set.add(gene_id) 
                else:
                    exp_gene_id_set.add(gene_id)
        set_list.append((nonexp_gene_id_set, exp_gene_id_set))
    return set_list

