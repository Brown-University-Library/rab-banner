"""
A script for processing course data from Banner.

Requires as input the LDAP file, and a data dump from Banner
in the form of a TSV file. Column headers in the TSV file should
line up with the headers listed in the function read_banner_csv.
Currently, the Banner dump includes the past nine semesters of 
Banner course data.

Faculty identity is drawn from VIVO with a SPARQL lookup. The 
function get_vivo_shortIDs returns shortIDs for faculty, which are 
mapped into LDAP to return their bruIDs.

The main function in the ingest is row_cleanup. Here, Banner data is
cleaned, rearranged, and merged before it is brought into VIVO.
Currently, all courses with the same label and taught by the same 
teacher are being merged into the same course (referenced with the 
same URI).

Currently, courses taught by multiple instructors -- the same course label,
number, department, etc., but with more than 1 instructor -- are being
treated as different courses.

Currently, no associations are being drawn between courses and 
departments The logic is still present, but is commented out.
"""

import os
import sys

from rdflib import Graph, URIRef, Literal, Namespace
from rdflib import RDF, RDFS, XSD, OWL

import csv
import json
import collections
import uuid
import requests
import pprint

import ldap_client
from config import settings
#from departmentMap import deptCodeMap

query_url = settings.config['RAB_QUERY_API']
email = settings.config['ADMIN_EMAIL']
passw = settings.config['ADMIN_PASS']
logDir = os.path.join(os.getcwd(),'log')

vivoName = "http://vivo.brown.edu/individual/"
VIVO = Namespace('http://vivoweb.org/ontology/core#')
VITRO = Namespace('http://vitro.mannlib.cornell.edu/ns/vitro/0.7#')
BLOCAL = Namespace('http://vivo.brown.edu/ontology/vivo-brown/')


#Global variables populated by refine_courseRows function,
#including academic terms, subjects, departments, and faculty members.
#These values are repeatedly referenced by multiple courses,
#so it seemed mappings global mappings were in order.
termMap = {}
courseMap = collections.defaultdict(dict)

#The list of statements that will be added to the RDF graph
statements = []

# def UnicodeDictReader(utf8_data, **kwargs):
#     csv_reader = csv.DictReader(utf8_data, **kwargs)
#     for row in csv_reader:
#         yield dict((key, value)
#             for key, value in row.items() if type(value) != list)

def read_banner_csv(bannerIn):
    headers=['TERM CODE',
        'TERM CODE DESCRIPTION',
        'CRN',
        'SUBJECT CODE',
        'SUBJECT CODE DESCRIPTION',
        'COURSE NUMBER',
        'SECTION NUMBER',
        'SECTION ENROLLMENT COUNT',
        'DEPARTMENT CODE OFFERING COURSE',
        'DEPARTMENT CODE DESCRIPTION',
        'COURSE TITLE',
        'COURSE DESCRIPTION',
        'INSTRUCTOR BROWN ID',
        'PRIMARY INSTRUCTOR',
        'GRADUATE STUDENT',
        'INSTRUCTOR NAME',
        'EMPTY_DELIMITER'
        ]
    bannerRowList = []
    with open(bannerIn, 'r', encoding='Windows-1252') as bannerCsv:
        csvDictObj = csv.DictReader(bannerCsv, fieldnames=headers, delimiter='\t')
        for row in csvDictObj:
            del row['EMPTY_DELIMITER']
            bannerRowList.append(row)
    return bannerRowList

def get_vivo_shortIDs():
    # No longer filtering for active faculty
    # REMOVED
    # ?fac a vivo:FacultyMember .
    query = """
    PREFIX vivo: <http://vivoweb.org/ontology/core#>
    PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX blocal: <http://vivo.brown.edu/ontology/vivo-brown/>

    SELECT DISTINCT ?fac ?shortID
    WHERE
    {
      ?fac blocal:shortId ?shortID .
    }
    """
    headers = {'Accept': 'text/csv', 'charset':'utf-8'} 
    data = { 'email': email, 'password': passw, 'query': query }
    resp = requests.post(query_url, data=data, headers=headers)
    short_id_map = {}
    if resp.status_code == 200:
        rdr = csv.reader(resp.text.split('\n'), delimiter=',')
        next(rdr)
        for row in rdr:
            try:
                facURI = row[0]
                shortID = row[1]
                short_id_map[shortID] = facURI
            except IndexError:
                continue
            except:
                raise Exception
    else:
        raise Exception("Bad query!")
    return short_id_map

def get_ldap_ids(courseRows):
    bruids = { row['INSTRUCTOR BROWN ID'] for row in courseRows }
    bruid_map = {}
    ldap_log = {}
    for bruid in bruids:
        ldap_attrs = ldap_client.by_id(bruid)
        ldap_log[bruid] = ldap_attrs
        try:
            shortid = ldap_attrs['brownshortid']
        except KeyError:
            print("No shortid mapped from {}".format(bruid))
            shortid = None
        bruid_map[bruid] = shortid
    with open(os.path.join(logDir, 'ldap_index.json'),'w') as f:
        json.dump(ldap_log, f, sort_keys=True, indent=4)
    return bruid_map

def log_skipped_rows(courses, shortURIMap, bruShortMap):
    skipped = []
    for row in courses:
        try:
            shortid = bruShortMap[row['INSTRUCTOR BROWN ID']] # In LDAP
            url = shortURIMap[shortid] #In VIVO
        except:
            skipped.append(row)
    with open(os.path.join(logDir, 'skipped_rows.csv'),'w') as f:
        fieldnames = list(skipped[0].keys())
        wrtr = csv.DictWriter(f, fieldnames=fieldnames)
        wrtr.writeheader()
        for row in skipped:
            wrtr.writerow(row)

def map_banner_ids(courseRow, shortURIMap, bruShortMap):
    try:
        shortid = bruShortMap[courseRow['INSTRUCTOR BROWN ID']]
        url = shortURIMap[shortid]
    except:
        return None
    courseRow['shortId'] = shortid
    courseRow['teacherURI'] = URIRef(url)
    return courseRow

def clean_title(courseTitle):
    title = " ".join(courseTitle.split())
    return title

def make_uuid_uri(base, prefix):
    for c in range(0,10):
        new_uri = '{0}{1}-{2}'.format(base, prefix, uuid.uuid4().hex)
        header = {'Accept': 'text/csv', 'charset':'utf-8'}
        query = "ASK {{<{0}> ?p ?o}}"
        data = {'email': email, 'password': passw, 'query': query.format(new_uri)}
        resp = requests.post(query_url, data=data, headers=header)
        existing = resp.content.decode('utf-8')
        if existing.strip().endswith('false'):
            return URIRef(new_uri)
        else:
            continue
    return None

def row_cleanup(courseRow):
    '''
    The essential function for course munging

    Of particular importance are courseKey and courseMap.
    The relationship defines what quaifies as a course.
    Currently, the key is the shortID, which maps to a courseLabel.
    This means that any course with the same label, taught by
    the same teacher, will be considered the same course.
    Previously, the key was (shortId, termCode); meaning that any 
    course with the same label, taught by the same teacher in 
    the same semester, would be considered the same course.
    '''
    shortId = courseRow['shortId']
    termCode = courseRow['TERM CODE']
    termLabel = courseRow['TERM CODE DESCRIPTION']
    subjCode = courseRow['SUBJECT CODE']
    courseTitle = courseRow['COURSE TITLE']
    courseNum = courseRow['COURSE NUMBER']

    #deptCode = courseRow['DEPARTMENT CODE OFFERING COURSE']
    #deptNum = deptCodeMap[deptCode]
    #courseRow['deptNum'] = deptNum

    termURI = URIRef(vivoName + "termcode-%s" % termCode)
    termMap[termCode] = {
                        'termCode': termCode,
                        'label': termLabel,
                        'URI': termURI,
                        }
    
    title = clean_title(courseTitle)
    courseKey = (shortId)
    courseLabel = subjCode + " " + courseNum + " - " + title
    if courseLabel in courseMap[courseKey]:
        courseURI = courseMap[courseKey][courseLabel]
    else:
        courseURI = make_uuid_uri(vivoName, prefix='course')
        if courseURI is None:
            raise Exception("Failed to generate new uri")
        courseMap[courseKey][courseLabel] = courseURI

    courseRow['courseLabel'] = courseLabel
    courseRow['courseURI'] = courseURI
    #courseRow['deptURI'] = URIRef(vivoName + "org-brown-univ-dept%s" % deptNum)
    courseRow['termURI'] = termURI

    return courseRow

def check_date(termCode):
    year = termCode[:4]
    month = termCode[-2:]

    if month == '15' or month == '20' or month == '29':
        yearNum = int(year) + 1
        year = str(yearNum)

    startMap = {'00': '06-01T00:00:00', '09': '06-01T00:00:00',
                '10': '09-01T00:00:00', '15': '01-01T00:00:00',
                '19': '09-01T00:00:00', '20': '02-01T00:00:00',
                '29': '02-01T00:00:00'
                }
    endMap  = { '00': '08-31T00:00:00', '09': '08-31T00:00:00',
                '10': '12-31T00:00:00', '15': '01-31T00:00:00',
                '19': '12-31T00:00:00', '20': '05-31T00:00:00',
                '29': '05-31T00:00:00'
                }

    startVal = "%s-%s" % (year, startMap[month])
    endVal = "%s-%s" % (year, endMap[month])
    startURI = URIRef(vivoName + "termstart-%s" % termCode)
    endURI = URIRef(vivoName + "termend-%s" % termCode)

    return (startURI, startVal, endURI, endVal)

def write_term_rdf():
    for termDict in termMap.values():

        startURI, startVal, endURI, endVal = check_date(termDict['termCode'])

        statements.extend([
            (termDict['URI'], RDF.type, VIVO['AcademicTerm']),
            (termDict['URI'], RDF.type, OWL['Thing']),
            (termDict['URI'], RDF.type, VIVO['DateTimeInterval']),
            (termDict['URI'], VITRO.mostSpecificType, VIVO['AcademicTerm']),
            (termDict['URI'], RDFS.label, Literal(termDict['label'])),

            (startURI, RDF.type, VIVO['DateTimeValue']),
            (startURI, RDF.type, OWL['Thing']),
            (startURI, VITRO.mostSpecificType, VIVO['DateTimeValue']),
            (startURI, RDFS.label, Literal(startVal)),
            (startURI, VIVO['dateTime'], Literal(startVal, datatype=XSD.dateTime)),
            (startURI, VIVO['dateTimePrecision'], VIVO['yearMonthDayPrecision']),
            (termDict['URI'], VIVO['start'], startURI),

            (endURI, RDF.type, VIVO['DateTimeValue']),
            (endURI, RDF.type, OWL['Thing']),
            (endURI, VITRO.mostSpecificType, VIVO['DateTimeValue']),
            (endURI, RDFS.label, Literal(endVal)),
            (endURI, VIVO['dateTime'], Literal(endVal, datatype=XSD.dateTime)),
            (endURI, VIVO['dateTimePrecision'], VIVO['yearMonthDayPrecision']),
            (termDict['URI'], VIVO['end'], endURI),
            ])

def write_course_rdf(cleanRows):
    for courseRow in cleanRows:
        
        statements.extend([
            (courseRow['courseURI'], RDF.type, VIVO['Course']),
            (courseRow['courseURI'], RDF.type, OWL['Thing']),
            (courseRow['courseURI'], VITRO.mostSpecificType, VIVO['Course']),
            (courseRow['courseURI'], RDFS.label, Literal(courseRow['courseLabel'])),
            #(courseRow['courseURI'], VIVO['courseOfferedBy'], courseRow['deptURI']),
            #(courseRow['deptURI'], VIVO['offersCourse'], courseRow['courseURI']),            
            (courseRow['courseURI'], VIVO['dateTimeInterval'], courseRow['termURI']),
            (courseRow['teacherURI'], BLOCAL['teacherFor'], courseRow['courseURI']),
            (courseRow['courseURI'], BLOCAL['hasTeacher'], courseRow['teacherURI']),
            ])

#The top-level function
def main(inFile, outFile):
    g = Graph()
    g.bind("blocal",BLOCAL)
    g.bind("vivo",VIVO)
    g.bind("vitro", VITRO)
    g.bind("owl", OWL)

    print("Data Prep")
    banner_rows = read_banner_csv(inFile)
    auth_map = get_vivo_shortIDs()
    ldap_map = get_ldap_ids(banner_rows)

    log_skipped_rows(banner_rows, auth_map, ldap_map)    
    mapped_rows = [ map_banner_ids(row, auth_map, ldap_map)
                    for row in banner_rows ]
    print("Munging data")
    cleaned_rows = [ row_cleanup(row) for row in mapped_rows if row ]
    print("Writing RDF")
    write_term_rdf()
    write_course_rdf(cleaned_rows)

    for stmt in statements:
        g.add(stmt)
    print(g.serialize(destination=outFile, format='n3'))


if __name__ == "__main__":
    in_file = sys.argv[1]
    out_file = sys.argv[2]
    main(in_file, out_file)
