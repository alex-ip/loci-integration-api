# -*- coding: utf-8 -*-
#
import asyncio
from aiohttp import ClientSession
from config import TRIPLESTORE_CACHE_SPARQL_ENDPOINT
from json import loads


async def query_graphdb_endpoint(sparql, infer=True, same_as=True, limit=1000, offset=0):
    """
    Pass the SPARQL query to the endpoint. The endpoint is specified in the config file.

    :param sparql: the valid SPARQL text
    :type sparql: str
    :param infer:
    :type infer: bool
    :param same_as:
    :type same_as: bool
    :param limit:
    :type limit: int
    :param offset:
    :type offset: int
    :return:
    :rtype: dict
    """
    loop = asyncio.get_event_loop()
    try:
        session = query_graphdb_endpoint.session_cache[loop]
    except KeyError:
        session = ClientSession(loop=loop)
        query_graphdb_endpoint.session_cache[loop] = session
    args = {
        'query': sparql,
        'infer': 'true' if bool(infer) else 'false',
        'sameAs': 'true' if bool(same_as) else 'false',
        'limit': int(limit),
        'offset': int(offset),
    }
    headers = {
        'Accept': "application/sparql-results+json,*/*;q=0.9",
        'Accept-Encoding': "gzip, deflate",
    }
    resp = await session.request('POST', TRIPLESTORE_CACHE_SPARQL_ENDPOINT, data=args, headers=headers)
    resp_content = await resp.text()
    return loads(resp_content)
query_graphdb_endpoint.session_cache = {}


async def get_resource(resource_uri):
    """
    :param resource_uri:
    :type resource_uri: str
    :return:
    """
    sparql = """\
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT DISTINCT ?p ?o ?p1 ?o1 ?p2 ?o2
WHERE {
    {
        ?s rdf:subject <URI> ;
           rdf:predicate ?p;
           rdf:object ?o .
        OPTIONAL { FILTER (isBlank(?o))
            {
                ?s2 rdf:subject ?o ;
                rdf:predicate ?p;
                rdf:object ?o1 .
            }
            UNION
            { ?o ?p1 ?o1 . }
            OPTIONAL { FILTER (isBlank(?o1))
                ?o1 ?p2 ?o2 .
            }
        }
    }
    UNION
    {
        <URI> ?p ?o .
        OPTIONAL { FILTER (isBlank(?o))
            {
                ?s3 rdf:subject ?o ;
                rdf:predicate ?p;
                rdf:object ?o1 .
            }
            UNION
            { ?o ?p1 ?o1 . }
            OPTIONAL { FILTER (isBlank(?o1))
                ?o1 ?p2 ?o2 .
            }
        }
    }
}
"""
    sparql = sparql.replace("<URI>", "<{}>".format(str(resource_uri)))
    resp = await query_graphdb_endpoint(sparql)
    resp_object = {}
    if 'results' not in resp:
        return resp_object
    bindings = resp['results']['bindings']
    for b in bindings:
        pred = b['p']['value']
        obj = b['o']
        if obj['type'] == "bnode":
            try:
                obj = resp_object[pred]
            except KeyError:
                resp_object[pred] = obj = {}
            pred1 = b['p1']['value']
            obj1 = b['o1']
            if obj1['type'] == "bnode":
                try:
                    obj1 = obj[pred1]
                except KeyError:
                    obj[pred1] = obj1 = {}
                pred2 = b['p2']['value']
                obj2 = b['o2']['value']
                obj1[pred2] = obj2
            else:
                obj1 = obj1['value']
            obj[pred1] = obj1
        else:
            obj = obj['value']
        resp_object[pred] = obj
    return resp_object


async def get_linksets(count=1000, offset=0):
    """
    :param count:
    :type count: int
    :param offset:
    :type offset: int
    :return:
    :rtype: tuple
    """
    sparql = """\
PREFIX loci: <http://linked.data.gov.au/def/loci#>
SELECT ?l
WHERE {
    ?l a loci:Linkset .
}
"""
    resp = await query_graphdb_endpoint(sparql, limit=count, offset=offset)
    linksets = []
    if 'results' not in resp:
        return linksets
    bindings = resp['results']['bindings']
    for b in bindings:
        linksets.append(b['l']['value'])
    meta = {
        'count': len(linksets),
        'offset': offset,
    }
    return meta, linksets

async def get_datasets(count=1000, offset=0):
    """
    :param count:
    :type count: int
    :param offset:
    :type offset: int
    :return:
    :rtype: tuple
    """
    sparql = """\
PREFIX dcat: <http://www.w3.org/ns/dcat#>
SELECT ?d
WHERE {
    ?d a dcat:Dataset .
}
"""
    resp = await query_graphdb_endpoint(sparql, limit=count, offset=offset)
    datasets = []
    if 'results' not in resp:
        return datasets
    bindings = resp['results']['bindings']
    for b in bindings:
        datasets.append(b['d']['value'])
    meta = {
        'count': len(datasets),
        'offset': offset,
    }
    return meta, datasets

async def get_locations(count=1000, offset=0):
    """
    :param count:
    :type count: int
    :param offset:
    :type offset: int
    :return:
    :rtype: tuple
    """
    sparql = """\
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX prov: <http://www.w3.org/ns/prov#>
SELECT DISTINCT ?l
WHERE {
    { ?l a geo:Feature }
    UNION
    { ?l a prov:Location } .
}
"""
    resp = await query_graphdb_endpoint(sparql, limit=count, offset=offset)
    locations = []
    if 'results' not in resp:
        return locations
    bindings = resp['results']['bindings']
    for b in bindings:
        locations.append(b['l']['value'])
    meta = {
        'count': len(locations),
        'offset': offset,
    }
    return meta, locations


async def get_location_is_within(target_uri, count=1000, offset=0):
    """
    :param target_uri:
    :type target_uri: str
    :param count:
    :type count: int
    :param offset:
    :type offset: int
    :return:
    :rtype: tuple
    """
    sparql = """\
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX prov: <http://www.w3.org/ns/prov#>
SELECT DISTINCT ?l
WHERE {
    {
        ?s rdf:subject <URI> ;
           rdf:predicate geo:sfWithin ;
           rdf:object ?l  .
    }
    UNION
    { <URI> geo:sfWithin ?l }
}
"""
    sparql = sparql.replace("<URI>", "<{}>".format(str(target_uri)))
    resp = await query_graphdb_endpoint(sparql, limit=count, offset=offset)
    locations = []
    if 'results' not in resp:
        return locations
    bindings = resp['results']['bindings']
    for b in bindings:
        locations.append(b['l']['value'])
    meta = {
        'count': len(locations),
        'offset': offset,
    }
    return meta, locations

async def get_location_contains(target_uri, count=1000, offset=0):
    """
    :param target_uri:
    :type target_uri: str
    :param count:
    :type count: int
    :param offset:
    :type offset: int
    :return:
    :rtype: tuple
    """
    sparql = """\
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX prov: <http://www.w3.org/ns/prov#>
SELECT DISTINCT ?l
WHERE {
    {
        ?s rdf:subject <URI> ;
           rdf:predicate geo:sfContains;
           rdf:object ?l  .
    }
    UNION
    { <URI> geo:sfContains ?l }
}
"""
    sparql = sparql.replace("<URI>", "<{}>".format(str(target_uri)))
    resp = await query_graphdb_endpoint(sparql, limit=count, offset=offset)
    locations = []
    if 'results' not in resp:
        return locations
    bindings = resp['results']['bindings']
    for b in bindings:
        locations.append(b['l']['value'])
    meta = {
        'count': len(locations),
        'offset': offset,
    }
    return meta, locations
