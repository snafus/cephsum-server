from xml.dom.minidom import parse
import xml.dom.minidom

import os,logging,re

def convert_path(path, xmlfile=None):
    """
    Convert  provided path (LFN) to pool and oid (PFN), using xmlfile for mapping if provided
    Parameters:
    path : input lfn path, e.g. from xrootd
    xmlfile : the xrootd xml file used to define any lfn to pfn mapping
    """
    if xmlfile is None:
        # No mapping to give, so assume the basic defaults
        lfn2pfn_converter = lfn2pfn.Lfn2PfnMapper() 
    else:
        lfn2pfn_converter = lfn2pfn.Lfn2PfnMapper.from_file(args.lfn2pfn_xmlfile)
    pool, path = lfn2pfn_converter.parse(lfn_path)

    return pool, path
    
def naive_ral_split_path(input_path):
    """Convert a path into pool and path using basic hardcoded logic.

    Check for a CMS style path, else assume follows pool:oid logic
    """
    nominal = re.compile("^/*([a-zA-Z0-9_-]+):(.*)")
    cms     = re.compile("^/*(store.*)")
    m = cms.match(input_path)
    if m is not None:
        return 'cms', m.group(1)

    m = nominal.match(input_path)
    if m is not None:
        return m.group(1), m.group(2)
    
    logging.error("Pool and path not available: %s", input_path)
    raise ValueError("Path not valid / or Not Implemented %s", input_path)



class Lfn2PfnMapper:
    """
    Use a nominal storage.xml file or xml string to convert LFN to PFNs for xroot
    
    Using only the direct protocol mappings, translate the path name provided into
    the pool name and object name.
    
    Can accept either xml file, or xml string to create the object; else mappings can be
    provided in (re.compile(...), result) tuple format to the self.mappings list.
    
    The first match found wins, and the path is converted according to the result string.
    (only a single substitution is possible in result)
    If no match is found in the mappings, then tries the nominal splitting.
    Even if a match is found, the nominal splitting is made to the mapped object. 
    This splits the (converted) path into pool name, oid name
    
    Parse method returns a tuple of pool and path
    """
    def __init__(self):
        """
        Trivial init script. Use the classmethods to init from some xml source
        """
        self.mappings = []
        self.source = None
        
        self.nominal = re.compile("^/*([a-zA-Z0-9_-]+):(.*)")
        
        pass

    @staticmethod
    def _build_mappers(dom_collection):
        """
        Converts the lfn-to-pfn rows in the xml to python.
        
        This is a rather naive implementation, but should be sufficient in most simple situations.
        Override this if you need a more precise interpretation of the xml document.
        """
        mappers = []
        elements = dom_collection.getElementsByTagName('lfn-to-pfn') 
        for col in elements:
            if col.getAttribute('protocol') !=  'direct':
                continue
            pattern = re.compile(col.getAttribute('path-match'))
            result  = col.getAttribute('result')
            mappers.append((pattern,result))
        return mappers


    
    @classmethod
    def from_file(cls,xmlfile):
        """
        Instantiate an object based on an xml file
        """
        if not os.path.exists(xmlfile):
            raise FileNotFoundError(f"Xml file: {xmlfile} Not Found!")

        DOMTree = xml.dom.minidom.parse(xmlfile)
        collection = DOMTree.documentElement
        mappers = Lfn2PfnMapper._build_mappers(collection)
        
        converter = cls()
        converter.mappings = mappers
        converter.source = xmlfile
        return converter
    
    @classmethod
    def from_string(cls,xmlstring):
        """
        Instantiate an object based on an xml string
        """

        DOMTree = xml.dom.minidom.parseString(xmlstring)
        collection = DOMTree.documentElement
        mappers = Lfn2PfnMapper._build_mappers(collection)
        
        converter = cls()
        converter.mappings = mappers
        converter.source = 'string'
        return converter


        
    def parse(self, pathname):
        """
        Parse a given LFN based on input rules
        
        Parameters
        ----------
        pathname: lfn, as provided by xrootd call
        
        Returns
        ----------
        pool name, oid name : tuple
        
        Raises
        ---------
        ValueError: pathname is not convertable
        """
        for mapper in self.mappings:
            match = mapper[0].match(pathname)
            #logging.debug(f'{mapper[0]}, {match}, {pathname}')
            if match is None:
                continue
            # replace occurences of $1, $2, etc. with their matching regex group
            # e.g.: newpath = mapper[1].replace('$1',match.group(1))
            # First extract the whole path
            newpath = mapper[1]
            placeholder_group = 1
            n_groups = len(match.groups())
            # Now loop over each $x and replace
            while True:
                placeholder = "${}".format(placeholder_group)
                if not placeholder in newpath:
                    break
                if placeholder_group > n_groups:
                    raise RuntimeError(f"Only {n_groups} available, but trying to replace {placeholder}")
                newpath = newpath.replace(placeholder, match.group(placeholder_group))
                placeholder_group += 1
            break
        else:
            # no match found in mappings, so just try with pathname
            logging.debug('No mapping matched; trying nominal')
            newpath = pathname
            
        fmatch = self.nominal.match(newpath)
        if fmatch is None:
            raise ValueError(f'Could not convert lfn-2-pfn for {pathname}')
        pool, oid = fmatch.group(1), fmatch.group(2)
        return pool,oid


    def __str__(self):
        return f'Lfn2PfnMapper: from {self.source},  mappings: {[x[0].pattern for x in self.mappings]}' 
            
       