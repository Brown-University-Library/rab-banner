import ldap

from config import settings

LDAP_USER = settings.config['LDAP_USER']
LDAP_PASSWORD = settings.config['LDAP_PASSWORD']

#setup ldap
l = ldap.initialize('ldap://directory.brown.edu')
username = "cn={},ou=special users,dc=brown,dc=edu".format(LDAP_USER)
password = LDAP_PASSWORD
try:
    l.protocol_version = ldap.VERSION3
    l.simple_bind_s(username, password)
    valid = True
except:
    raise

def _f(attrs, key):
    """
    Helper to pull LDAP attributes.
    """
    try:
        return attrs[key][0]
    except:
        return None

def read_results(rsp):
    """
    Put LDAP response into a dictionary.
    """
    out = {}
    for item in rsp:
        for k,v in item[1].items():
            try:
                out[k] = v[0].decode('utf-8')
            except:
                raise UnicodeError(v[0])
    return out

def run_search(search_pair):
    base_dn = 'ou=people,dc=brown,dc=edu'
    attrs = ['brownshortid', 'brownuuid', 'brownbruid', 'mail', 'brownshortid']
            #['telephonenumber', 'title']
    results = l.search_s( base_dn, ldap.SCOPE_SUBTREE, search_pair, attrs )
    d = read_results(results)
    return d

def by_id(brown_id):
    """
    Query ldap by short id to get back brownuid and email.
    """
    search_pair = '(brownbruid=%s)' % brown_id
    d = run_search(search_pair)
    return d

def by_shortId(short):
    """
    Query ldap by short id to get back brownuid and email.
    """
    search_pair = '(brownshortid=%s)' % short
    d = run_search(search_pair)
    return d

def by_uuid(value):
    search_pair = '(brownuuid=%s)' % value
    d = run_search(search_pair)
    return d


if __name__ == "__main__":
    pass
