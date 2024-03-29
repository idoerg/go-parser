import MySQLdb

def mysql_query(mysql_template, arg_tuple, cursor):
    sql = mysql_template % arg_tuple
    cursor.execute(sql)
    results = cursor.fetchall()
    return results

def scalarp(item,scalar_example):
    return (type(item) == type(scalar_example) or
                item is None)
def flatten(sequence,scalarp,result=None):
    if result is None: result = []
    for item in sequence:
        if scalarp(item,''):
            result.append(item)
        else:
            flatten(item, scalarp, result)
    return result
def open_go(host="localhost",user="idoerg",passwd="mingus",db="MyGO"):
    tsdb_con = MySQLdb.connect(host, user, passwd, db)
    tsdb_cursor = tsdb_con.cursor()
    return tsdb_con, tsdb_cursor

sql_go_city_block = \
"""
SELECT 
  min(graph_path1.distance + graph_path2.distance) AS dist
FROM 
  graph_path AS graph_path1, graph_path AS graph_path2, 
  term AS t1, term AS t2
WHERE
  t1.acc = '%s' and t2.acc = '%s' and graph_path1.term2_id = t1.id
  and graph_path2.term2_id = t2.id and graph_path1.term1_id = graph_path2.term1_id;
"""

def go_city_block(acc1, acc2, go_cursor):
    retval = mysql_query(sql_go_city_block, (acc1, acc2), go_cursor)[0][0]
    if not retval: 
        retval = 0.
    return float(retval)

def go_level(acc, go_cursor, root_acc='all'):
    level = go_city_block(acc, root_acc, go_cursor)
    return level
def go_distance_path_1(acc1, acc2, go_cursor, root_acc='all'):
    cbd = go_city_block(acc1, acc2, go_cursor)
    level1 = go_level(acc1, root_acc, go_cursor)
    level2 = go_level(acc2, root_acc, go_cursor)
    go_distance = 2*(cbd + 1) / (level1+level2)
    return go_distance

def go_similarity_path_1(acc1,acc2,go_cursor,root_acc='all'):
    return 1.0/go_distance_1

def go_acc_to_name(acc, go_cursor):
    name = mysql_query("SELECT name FROM term WHERE acc='%s';",
                      (acc,), go_cursor)[0][0]
    return name                      
def go_acc_to_term_type(acc, go_cursor):
    # find out which ontology this GO term belongs to
    term_type = mysql_query("SELECT term_type FROM term WHERE acc='%s';",
                      (acc,), go_cursor)[0][0]
    return  term_type                     
sql_get_ancestors = \
"""
SELECT DISTINCT
        ancestor.*, 
        graph_path.term1_id AS ancestor_id
 FROM 
  term
  INNER JOIN graph_path ON (term.id=graph_path.term2_id)
  INNER JOIN term AS ancestor ON (ancestor.id=graph_path.term1_id)
 WHERE term.acc='%s';
"""

def is_ancestor(son_acc, ancestor_acc, go_cursor):
    # Finds whether the son GO accession number is a descendant of the ancestor GO
    # accession number.
    ancestors_accessions = {}
    ancestors = mysql_query(sql_get_ancestors,(son_acc,), go_cursor)
    for i in ancestors:
        ancestors_accessions[i[3]] = None
    if ancestor_acc in ancestors_accessions:
        return True
    else:
        return False

def go_acc_to_synonym_name(acc, go_cursor):
	name = mysql_query("SELECT name FROM term, term_synonym WHERE acc_synonym='%s'\
								AND id = term_id;", (acc), go_cursor)[0][0]
	return name      

