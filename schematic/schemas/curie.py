"""CURIE - A Module for handling Compact URIs (CURIEs)

Features:
- Support for RDFLib datatypes (BNode, URIRef), but doesn't require it
- MIT licensed

CURIE Syntax specification:
    <http://www.w3.org/TR/2007/WD-curie-20070307/>
Blank node handling notes:
    <http://milicicvuk.com/blog/2011/08/18/extended-curie-prefixlocalnamekey/>

Copyright (C) 2011-2012 Jaakko Salonen

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

"""

__author__ = "Jaakko Salonen"
__copyright__ = "Copyright 2011-2012, Jaakko Salonen"
__version__ = "0.5.0"
__license__ = "MIT"
__status__ = "Prototype"

from urllib.parse import unquote
from copy import copy
from rdflib import BNode, URIRef

import sys
if sys.version_info.major == 3:
    unicode = str


try:
    from rdflib import BNode, URIRef
except:
    # Fallback if rdflib is not present
    class BNode(object):
        def __init__(self, val):
            self.val = val
        def n3(self):
            return unicode('_:'+self.val)
    class URIRef(unicode): pass

class Curie(object):
    """ Curie Datatype Class

    Examples:
        
    >>> nss = dict(dc='http://purl.org/dc/elements/1.1/')
        
    >>> dc_title = Curie('http://purl.org/dc/elements/1.1/title', nss)
        
    >>> dc_title.curie
    u'dc:title'
    
    >>> dc_title.uri 
    u'http://purl.org/dc/elements/1.1/title'

    >>> dc_title.curie
    u'dc:title'
     
    >>> nss['example'] = 'http://www.example.org/'
     
    >>> iri_test = Curie('http://www.example.org/D%C3%BCrst', nss)
     
    >>> iri_test.uri
    u'http://www.example.org/D\\xfcrst'
     
    >>> iri_test.curie
    u'example:D%C3%BCrst'

    """
        
    def __init__(self, uri, namespaces=dict()):
            self.namespaces = namespaces
            self.uri = unicode(unquote(uri), 'utf-8')
            self.curie = copy(self.uri)
            for ns in self.namespaces:
                    self.curie = uri.replace(u''+self.namespaces['%s'%ns], u"%s:" % ns)
    
    def __str__(self):
            return self.__unicode__()
    
    def __unicode__(self):
            return self.curie

def uri2curie(uri, namespaces):
    """ Convert URI to CURIE

        Define namespaces we want to use:
        >>> nss = dict(dc='http://purl.org/dc/elements/1.1/')

        Converting a string URI to CURIE
        >>> uri2curie('http://purl.org/dc/elements/1.1/title', nss)
        u'dc:title'

        RDFLib data type conversions:

        URIRef to CURIE
        >>> uri2curie(URIRef('http://purl.org/dc/elements/1.1/title'), nss)
        u'dc:title'

        Blank node to CURIE
        >>> uri2curie(BNode('blanknode1'), nss)
        u'_:blanknode1'

    """

    # Use n3() method if BNode
    if isinstance(uri, BNode):       
        result = uri.n3()
    else:
        result = uri

    #  result = unicode(uri)    
    for ns in namespaces:
            ns_raw = u'%s' % namespaces['%s'%ns]
            if ns_raw == 'http://www.w3.org/2002/07/owl#uri':
                    ns_raw = 'http://www.w3.org/2002/07/owl#'
            result = result.replace(ns_raw, u"%s:" % ns)
            
    result = result.replace(u'http://www.w3.org/2002/07/owl#', 'owl:')
    return result

def curie2uri(curie, namespaces):
    """ Convert CURIE to URI

        TODO: testing

    """
    result = unicode(curie)
    for ns in namespaces:
            result = result.replace(u"%s:" % ns, u''+namespaces['%s'%ns])
    return URIRef(result)

if __name__ == "__main__":
        import doctest
        doctest.testmod()
