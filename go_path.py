import UserDict
import MySQLdb
from go_utils import flatten,scalarp, mysql_query
## MySQL query templates

# graph_distance_sql: find the minimum distance between any two ontologies,
# given their acc codes
graph_distance_sql = """
	SELECT 
  		min(graph_path1.distance + graph_path2.distance) AS dist
	FROM 
  		graph_path AS graph_path1, graph_path AS graph_path2, 
  		term AS t1, term AS t2
	WHERE
  		t1.acc = "%s" and t2.acc = "%s" and
  		graph_path1.term2_id = t1.id and graph_path2.term2_id = t2.id and
  		graph_path1.term1_id = graph_path2.term1_id
	"""

# go_level_sql: distance of a node from its root
# (molecular_function / cellular_component / biological_process)
go_level_sql = """
	SELECT 
  		min(graph_path1.distance + graph_path2.distance) AS dist
	FROM 
  		graph_path AS graph_path1, graph_path AS graph_path2, 
  		term AS t1, term AS t2
	WHERE
  		t1.name = "%s" and t2.acc = "%s" and
  		graph_path1.term2_id = t1.id and graph_path2.term2_id = t2.id and
  		graph_path1.term1_id = graph_path2.term1_id
	"""
get_parents_sql = \
"""
SELECT p.acc
FROM
 graph_path 
  INNER JOIN
 term AS t ON (t.id = graph_path.term2_id)
  INNER JOIN
 term AS p ON (p.id = graph_path.term1_id)
WHERE
	t.acc = "%s" AND
	graph_path.distance = 1;
	
"""
get_all_parents_sql = \
"""
SELECT p.acc
FROM
 graph_path 
  INNER JOIN
 term AS t ON (t.id = graph_path.term2_id)
  INNER JOIN
 term AS p ON (p.id = graph_path.term1_id)
WHERE
	t.acc = "%s"
"""

get_children_sql = \
"""
SELECT c.acc
FROM
 graph_path 
  INNER JOIN
 term AS t ON (t.id = graph_path.term1_id)
  INNER JOIN
 term AS c ON (c.id = graph_path.term2_id)
WHERE
	t.acc = "%s" AND
	graph_path.distance = 1;
	
"""
get_parent_relationship_sql = \
"""
SELECT p.acc,r.relationship_type_id
FROM
 graph_path 
  INNER JOIN
 term AS t ON (t.id = graph_path.term2_id)
  INNER JOIN
 term AS p ON (p.id = graph_path.term1_id)
  INNER JOIN
 term2term AS r ON (r.term2_id = p.id)
WHERE
	t.acc = '%s' AND
	graph_path.distance = 1;
"""

get_child_relationship_sql = \
"""
SELECT c.acc,r.relationship_type_id
FROM
 graph_path 
  INNER JOIN
 term AS t ON (t.id = graph_path.term1_id)
  INNER JOIN
 term AS c ON (c.id = graph_path.term2_id)
  INNER JOIN
 term2term AS r ON (r.term1_id = t.id)
WHERE
	t.acc = '%s' AND
	graph_path.distance = 1;
"""
synonym_acc_sql = \
"""
SELECT term.acc FROM 
	term, term_synonym 
WHERE
	term_synonym.term_synonym="%s" AND
	term_synonym.term_id = term.id;

"""
get_root_sql = \
"""
SELECT 
	t1.acc 
FROM 
	term AS t1 JOIN 
	term AS t2 
WHERE 
	t2.acc='%s' AND t1.name=t2.term_type;
"""

get_term_definition_sql= \
"""
SELECT 
	td.term_definition
FROM
	term_definition AS td JOIN
	term
WHERE
	term.acc = '%s' AND
	term.id = td.term_id;
"""
get_term_comment_sql= \
"""
SELECT 
	td.term_comment
FROM
	term_definition AS td JOIN
	term
WHERE
	term.acc = '%s' AND
	term.id = td.term_id;
"""
IS_A  = 1
PART_OF = 2

def open_go(user,passwd,db,host="localhost"):
	mygo_con = MySQLdb.connect(host=host,
                          user=user,
                          passwd=passwd,
                          db=db)
	return mygo_con.cursor()


class GO_DAG(UserDict.UserDict):
	def __init__(self, root=None,go_handle=None):
		# self.data: a dictionary whose indices are: parents' 
      # accession numbers (first 
      # dimension) and childrens' acess numbers (second dimension)
		# value holds the edge type: is-a / part-of
		#
		# nodes: a dictionary. Indexes: accession numbers; values:
		# GO_Node instances
		self.data = {}
		self.nodes = {}
		self.root = root
		self.go_handle = go_handle

	def populate(self, root_acc,go_handle):
		# Populate with an ontology DAG. From a root node onwards
		self.root = GO_node(root_acc,go_handle)
		self._populate_recursive(self.root)
	def _populate_recursive(self,parent):
		parent.set_children()
		children = parent.children
		for child in children:
			self[parent.acc,child.acc] = child[1] # edge type
			self.nodes[child.acc] = child[0] # node
			self._populate_recursive(child[0])
	def get_children(self, parent_acc):
		# Given a parent term in the DAG, get its immediate children terms
		children = []
		for i in self:
			if i[0] == parent_acc:
				children.append((self.nodes[i[1]], self[i]))
		return children
	def get_parents(self, child_acc):
		# given a term, get its immediate parent(s)
		parents = []
		for i in self:
			if i[1] == child_acc:
				parents.append((self.nodes[i[0]], self[i]))
		return parents

	
class GO_node:
	def __init__(self,acc,go_handle,is_root=0):
		self.acc = acc
		self.go_handle = go_handle
		self.name = ''
		self.root = None
		self.is_obsolete = False
		self.definition = None
		self.comment = None
		self.parents = {}
		self.all_parents_acc = []
		self.children = {}
		self._set_name_by_acc()
		self.set_definition()
		if not is_root:
			self.set_root()
		else:
			self.root = self
		self.check_acc()

	def check_acc(self):
		is_in_term = mysql_query("SELECT id from term where acc='%s';",
							           (self.acc,), self.go_handle)
		if not is_in_term:
			new_acc = self.fix_synonym()
			if not new_acc:
				raise ValueError, "bad GO acc %s" % self.acc
	 		else:
				self.acc = new_acc
		# Check if obsolete. Not sure what to do if it is.
		self.is_obsolete = bool(mysql_query("SELECT is_obsolete FROM term where acc='%s';",
								 (self.acc,), self.go_handle)[0][0])
	

	def fix_synonym(self):
		new_acc = mysql_query(synonym_acc_sql,(self.acc,),self.go_handle)[0][0]
		return new_acc
	def __repr__(self):
		return "%s <%s> (%s)" % (self.acc, self.name, self.root.name)
	def _set_name_by_acc(self):
		self.name = mysql_query("select name from term where term.acc = '%s'",
                   (self.acc), self.go_handle)[0][0]
		
	def set_root(self):
		# root_acc =  mysql_query(get_root_sql, (self.acc,), self.go_handle)
		parents = mysql_query(get_all_parents_sql, (self.acc,), self.go_handle)
		if ('GO:0005575',) in parents or ('obsolete_component',) in parents or \
			('obsolete_cellular_component',) in parents:
			self.root = self.__class__('GO:0005575',self.go_handle,is_root=1)
		elif ('GO:0008150',) in parents  or ('obsolete_process',) in parents or \
							 ('obsolete_biological_process',) in parents:
			self.root = self.__class__('GO:0008150', self.go_handle,is_root=1)
		elif ('GO:0003674',) in parents or ('obsolete_function',) in parents or \
							 ('obsolete_molecular_function',) in parents:
			self.root = self.__class__('GO:0003674', self.go_handle,is_root=1)
		else:
			raise ValueError, \
			"Problem with root node for %s. Parents %s" % (self.acc, parents)
		self.all_parents_acc = [i[0] for i in parents]
		self.all_parents_acc.remove(self.acc)

	def level(self):
		if not self.root:
			self.set_root()
		level = mysql_query(graph_distance_sql, 
                         (self.root.acc, self.acc), self.go_handle)[0][0]
		return int(level)

	def edge_distance(self, other):
		if self.root.acc != other.root.acc:
			return -1
		d = mysql_query(graph_distance_sql,
                  (self.acc, other.acc), self.go_handle)[0][0]
		return int(d)
	def set_children(self):
		c = mysql_query(get_child_relationship_sql, (self.acc,), self.go_handle)
		for child in c:
			relationship = mysql_query("select name from term where id='%s';",
                        (child[1],), self.go_handle)[0][0]
#			print child, relationship
			self.children[child[0]] = (self.__class__(child[0],self.go_handle), relationship)
			self.children[child[0]][0].parents[self.acc] = (self, relationship)
			
	def set_parents(self):
		p = mysql_query(get_parent_relationship_sql, (self.acc,), self.go_handle)
		for parent in p:
			relationship = mysql_query("select name from term where id='%s';",
                        (parent[1],), self.go_handle)[0][0]
			self.parents[parent[0]] = (self.__class__(parent[0], self.go_handle), relationship)

	def has_children(self):
		return bool(len(self.children.items()) > 0)
	def has_parents(self):
		return bool(len(self.parents.items()) > 0)
	def get_parents_acc(self):
		if not self.has_parents():
			self.set_parents()
		return self.parents.keys()
	def get_children_acc(self):
		if not self.has_children():
			self.set_children()
		return self.children.keys()
		
	def set_definition(self):
		definition = flatten(mysql_query(get_term_definition_sql,(self.acc,),self.go_handle),scalarp)
		if not definition:
			self.definition = ''
		else:
			self.definition = definition[0]

	def set_comment(self):
		comment = flatten(mysql_query(get_term_comment_sql,(self.acc,),self.go_handle),scalarp)
		if not comment:
			self.comment = ''
		else:
			self.comment = comment[0]

def get_all_ontology(this_node,go_handle,is_root):
	this_node.set_children()
	for child_node in this_node.children.values():
		root_node = get_all_ontology(child_node,go_handle,is_root=0)
	return this_node.root
	
	
class GO_nedge(GO_node):
	def __init__(self, acc, go_handle, edge_type):
		self.edge_type = edge_type
