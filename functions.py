import asyncio
import asyncpg
from decimal import Decimal
from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientConnectorError
from config import TRIPLESTORE_CACHE_SPARQL_ENDPOINT
from config import GEOBASE_ENDPOINT 
from config import ES_ENDPOINT
from config import DB_CONFIG
from config import USE_SQL
import re

from json import loads

from errors import ReportableAPIError

async def query_postgres(sql, limit=1000, offset=0):
    """
    Pass the SQL query to Postgres. The connection details are specified in the config file.

    :param sql: the valid SQL text
    :type sql: str
    :param limit:
    :type limit: int
    :param offset:
    :type offset: int
    :return:
    :rtype: dict
    """
    
    # Apply limit and offset values to query - this is a bit ugly
    if re.search('limit(\s+)(\d+)', sql, flags=re.IGNORECASE):
        sql = re.sub('limit(\s+)(\d+)', 'limit {}'.format(limit), sql, count=1, flags=re.IGNORECASE | re.DOTALL)
    else:
        sql = re.sub('(;?)(\s*)$', ' limit {};'.format(limit), sql, count=1, flags=(re.IGNORECASE | re.DOTALL))
        
    if re.search('offset(\s+)(\d+)', sql, flags=re.IGNORECASE):
        sql = re.sub('offset(\s+)(\d+)', 'offset {}'.format(offset), sql, count=1, flags=re.IGNORECASE | re.DOTALL)
    else:
        sql = re.sub('(;?)(\s*)$', ' offset {};'.format(offset), sql, count=1, flags=(re.IGNORECASE | re.DOTALL))
        
    #print(sql)
        
    rows = []
    if query_postgres.pool is None:
        # DSN string format: postgres://user:pass@host:port/database?option=value
        query_postgres.pool = await asyncpg.create_pool('postgresql://{user}:{password}@{host}:{port}/{dbname}'.format(
            host=DB_CONFIG['POSTGRES_SERVER'], 
            port=DB_CONFIG['POSTGRES_PORT'], 
            dbname=DB_CONFIG['POSTGRES_DBNAME'], 
            user=DB_CONFIG['POSTGRES_USER'], 
            password=DB_CONFIG['POSTGRES_PASSWORD']
            ), command_timeout=60, min_size=1, max_size=2)
    conn = await query_postgres.pool.acquire() 
    
    try:
        rows = await conn.fetch(sql)
    finally:
        await query_postgres.pool.release(conn)
        
    return rows

query_postgres.pool = None 



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
                rdf:predicate ?p1;
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
                rdf:predicate ?p1;
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
    if USE_SQL:
        location_sql = '''\
SELECT DISTINCT
    linkset_uri as linkset
from linkset 
order by linkset_uri
'''
        rows = await query_postgres(location_sql, limit=count, offset=offset)
        
        #print(rows)
        
        # include_areas, include_proportion, include_within, include_contains
        linksets = [
            {
                "uri": row['linkset'],
            } 
            for row in rows
            ]
    
        meta = {
            'count': len(rows),
            'offset': offset,
        }

    else: # Original SPARQL code
        
        sparql = """\
PREFIX loci: <http://linked.data.gov.au/def/loci#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT DISTINCT ?l
WHERE {
    {
        ?l a loci:Linkset .
    }
    UNION
    {
        ?c rdfs:subClassOf+ loci:Linkset .
        ?l a ?c .
    }
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
    if USE_SQL:
        dataset_sql = '''\
SELECT DISTINCT
    dataset_uri as dataset
from dataset 
order by dataset_uri
'''
        rows = await query_postgres(dataset_sql, limit=count, offset=offset)
        
        #print(rows)
        
        # include_areas, include_proportion, include_within, include_contains
        datasets = [
            {
                "uri": row['dataset'],
            } 
            for row in rows
            ]
    
        meta = {
            'count': len(rows),
            'offset': offset,
        }

    else: # Original SPARQL code
        
        sparql = """\
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT DISTINCT ?d
WHERE {
    {
        ?d a dcat:Dataset .
    }
    UNION
    {
        ?c rdfs:subClassOf+ dcat:Dataset .
        ?d a ?c .
    }
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
    if USE_SQL:
        location_sql = '''\
SELECT DISTINCT
    feature_uri as feature
from feature 
order by feature_uri
'''
        rows = await query_postgres(location_sql, limit=count, offset=offset)
        
        #print(rows)
        
        # include_areas, include_proportion, include_within, include_contains
        locations = [
            {
                "uri": row['feature'],
            } 
            for row in rows
            ]
    
        meta = {
            'count': len(rows),
            'offset': offset,
        }

    else: # Original SPARQL code
        
        sparql = """\
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT DISTINCT ?l
WHERE {
    { ?l a geo:Feature }
    UNION
    {
        ?c1 rdfs:subClassOf+ geo:Feature .
        ?l a ?c1 .
    }
    UNION
    {
        ?s1 rdf:subject ?l ;
            rdf:predicate rdf:type ;
            rdf:object geo:Feature .
    }
    UNION
    { ?l a prov:Location }
    UNION
    {
        ?c2 rdfs:subClassOf+ prov:Location .
        ?l a ?c2 .
    }
    UNION
    {
        ?s2 rdf:subject ?l ;
            rdf:predicate rdf:type ;
            rdf:object prov:Location .
    } .
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
    if USE_SQL:
        within_sql = '''\
SELECT DISTINCT
    f1.feature_uri as feature1, 
    f2.feature_uri as feature2
FROM (select *
      from feature 
      where feature_uri = '{feature_uri}'
     ) f1
INNER JOIN (select *
            from feature 
           ) f2     
    ON (ST_CoveredBy(f1.feature_geometry, f2.feature_geometry)
        AND f1.feature_id !=f2.feature_id
        )
order by feature1, feature2
'''.format(feature_uri=target_uri)

        rows = await query_postgres(within_sql, limit=count, offset=offset)
        
        #print(rows)
        
        # include_areas, include_proportion, include_within, include_contains
        locations = [
            {
                "uri": row['feature2'],
            } 
            for row in rows
            ]
    
        meta = {
            'count': len(rows),
            'offset': offset,
        }

    else: # Original SPARQL code
        
        sparql = """\
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT DISTINCT ?l
WHERE {
    {
        ?s rdf:subject <URI> ;
           rdf:predicate geo:sfWithin ;
           rdf:object ?l  .
    }
    UNION
    { <URI> geo:sfWithin+ ?l }
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
    if USE_SQL:
        contains_sql = '''\
SELECT DISTINCT
    f1.feature_uri as feature1, 
    f2.feature_uri as feature2
FROM (select *
      from feature 
      where feature_uri = '{feature_uri}'
     ) f1
INNER JOIN (select *
            from feature 
           ) f2     
    ON (ST_CoveredBy(f2.feature_geometry, f1.feature_geometry)
        AND f1.feature_id !=f2.feature_id
        )
order by feature1, feature2
'''.format(feature_uri=target_uri)

        rows = await query_postgres(contains_sql, limit=count, offset=offset)
        
        #print(rows)
        
        # include_areas, include_proportion, include_within, include_contains
        locations = [
            {
                "uri": row['feature2'],
            } 
            for row in rows
            ]
    
        meta = {
            'count': len(rows),
            'offset': offset,
        }

    else: # Original SPARQL code
        
        sparql = """\
    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    SELECT DISTINCT ?l
    WHERE {
        {
            ?s rdf:subject <URI> ;
               rdf:predicate geo:sfContains;
               rdf:object ?l  .
        }
        UNION
        { <URI> geo:sfContains+ ?l }
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

async def query_build_response_bindings(sparql, count, offset, bindings):
    """
    :param sparql:
    :type sparql: str
    :param count:
    :type count: int
    :param offset:
    :type offset: int
    :return:
    """
    resp = await query_graphdb_endpoint(sparql, limit=count, offset=offset)
    if 'results' in resp and 'bindings' in resp['results']:
        if len(resp['results']['bindings']) > 0:
            if len(resp['results']['bindings'][0].keys()) > 0:
                bindings.extend(resp['results']['bindings'])


async def get_location_overlaps(target_uri, include_areas, include_proportion, include_within, include_contains, count=1000, offset=0):
    """
    :param target_uri:
    :type target_uri: str
    :type include_areas: bool
    :type include_proportion: bool
    :type include_within: bool
    :type include_contains: bool
    :param count:
    :type count: int
    :param offset:
    :type offset: int
    :return:
    """
    
    if USE_SQL:
        #TODO: Optimise this query to only return quantitative results if required
        # PostGIS Methodology from https://postgis.net/2014/03/14/tip_intersection_faster/
        overlaps_sql = '''\
SELECT *,
intersection_area / feature1_area as feature1_proportion,
intersection_area / feature2_area as feature2_proportion
from (
SELECT 
    f1.feature_uri as feature1, 
    ST_Area(f1.feature_geometry) as feature1_area,
    f2.feature_uri as feature2, 
    ST_Area(f2.feature_geometry) as feature2_area,
    ST_Area(CASE 
        WHEN ST_CoveredBy(f1.feature_geometry, f2.feature_geometry) 
               THEN f1.feature_geometry
           WHEN ST_CoveredBy(f2.feature_geometry, f1.feature_geometry) 
               THEN f2.feature_geometry
           ELSE 
            ST_Multi(ST_Intersection(f1.feature_geometry, f2.feature_geometry))
        END) as intersection_area
FROM (select *
      from feature 
      where feature_uri = '{feature_uri}'
     ) f1
INNER JOIN (select *
            from feature 
           ) f2     
    ON (ST_Intersects(f1.feature_geometry, f2.feature_geometry) 
        AND NOT ST_Touches(f1.feature_geometry, f2.feature_geometry)
        AND f1.feature_id !=f2.feature_id
        )
) areas
order by feature1, feature2
'''.format(feature_uri=target_uri)

        rows = await query_postgres(overlaps_sql, limit=count, offset=offset)
        
        #print(rows)
        
        # include_areas, include_proportion, include_within, include_contains
        overlaps = []
        for row in rows:
            overlap = {
                "uri": row['feature2'],
                } 
            
            if include_areas:
                overlap["featureArea"] = row['feature2_area']
                overlap["intersectionArea"] = row['intersection_area']
            
            if include_proportion:
                overlap["forwardProportion"] = row['feature2_proportion']
                overlap["reverseProportion"] = row['feature1_proportion']
                
            if include_within:
                overlap["isWithin"] = (row['intersection_area'] == row['feature1_area'])
     
            if include_contains:
                overlap["contains"] = (row['intersection_area'] == row['feature2_area'])
     
            overlaps.append({key: str(value) for key, value in overlap.items()})
    
        meta = {
            'count': len(rows),
            'offset': offset,
        }
        if rows[0]['feature1_area'] and include_areas:
            meta['featureArea'] = str(rows[0]['feature1_area'])
    
    else: # Original SPARQL code

        overlaps_sparql = """\
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX geox: <http://linked.data.gov.au/def/geox#>
PREFIX qb4st: <http://www.w3.org/ns/qb4st/>
PREFIX epsg: <http://www.opengis.net/def/crs/EPSG/0/>
PREFIX dt: <http://linked.data.gov.au/def/datatype/>
SELECT <SELECTS>
WHERE {
    {
        {
           ?s1 rdf:subject <URI> ;
           rdf:predicate geox:transitiveSfOverlap;
           rdf:object ?o  .
        } UNION {
           ?s2 rdf:subject <URI> ;
           rdf:predicate geo:sfOverlaps;
           rdf:object ?o  .
        }
    }
    UNION
    { <URI> geox:transitiveSfOverlap ?o }
    UNION
    { <URI> geo:sfOverlaps ?o }
    <EXTRAS>
}
GROUP BY ?o
"""
        contains_sparql = """\
    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX geox: <http://linked.data.gov.au/def/geox#>
    PREFIX qb4st: <http://www.w3.org/ns/qb4st/>
    PREFIX epsg: <http://www.opengis.net/def/crs/EPSG/0/>
    PREFIX dt: <http://linked.data.gov.au/def/datatype/>
    SELECT ?c <SELECTS>
    WHERE {
        {  
            ?s2 rdf:subject <URI> ;
            rdf:predicate geo:sfContains;
            rdf:object ?o  .
        }
        UNION
        { <URI> geo:sfContains+ ?o }
        OPTIONAL { FILTER(bound(?o))
            BIND(true as ?c) .
        }
        <EXTRAS>
    }
    GROUP BY ?c ?o
"""
        within_sparql = """\
    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX geox: <http://linked.data.gov.au/def/geox#>
    PREFIX qb4st: <http://www.w3.org/ns/qb4st/>
    PREFIX epsg: <http://www.opengis.net/def/crs/EPSG/0/>
    PREFIX dt: <http://linked.data.gov.au/def/datatype/>
    SELECT ?w <SELECTS>
    WHERE {
        {  
            ?s2 rdf:subject <URI> ;
            rdf:predicate geo:sfWithin;
            rdf:object ?o  .
        }
        UNION
        { <URI> geo:sfWithin+ ?o }
        OPTIONAL { FILTER(bound(?o))
            BIND(true as ?w) .
        }
        <EXTRAS>
    }
    GROUP BY ?w ?o
"""
        use_areas_sparql = include_proportion or include_areas
        use_proportion_sparql = include_proportion
     
        selects = "?o "
        area_selects = "(MAX(?a1) as ?uarea) (MAX(?a2) as ?oarea) "
        iarea_selects = "(MAX(?a3) as ?iarea) "
     
        areas_sparql = """\
    OPTIONAL {
        <URI> geox:hasAreaM2 ?ha1 .
        ?ha1 qb4st:crs epsg:3577 .
        ?ha1 dt:value ?a1 .
    }
    OPTIONAL {
        ?o geox:hasAreaM2 ?ha2 .
        ?ha2 qb4st:crs epsg:3577 .
        ?ha2 dt:value ?a2 .
    }
"""
        iarea_sparql = """\
    OPTIONAL {
        { <URI> geo:sfContains ?i }
        UNION 
        {
            ?s3 rdf:subject <URI> ;
                rdf:predicate geo:sfContains ;
                rdf:object ?i 
        } .
         
        { ?o geo:sfContains ?i }
        UNION 
        {
            ?s4 rdf:subject ?o ;
                rdf:predicate geo:sfContains ;
                rdf:object ?i 
        } .
        OPTIONAL {
            ?i geox:hasAreaM2 ?ha3 .
            ?ha3 qb4st:crs epsg:3577 .
            ?ha3 dt:value ?a3 .
        }
    }
"""
        extras = ""
        use_selects = selects
        if use_areas_sparql:
            extras += areas_sparql
            use_selects += area_selects
        if use_proportion_sparql:
            extras += iarea_sparql
            use_selects += iarea_selects
        sparql = overlaps_sparql.replace("<SELECTS>", use_selects)
        sparql = sparql.replace("<EXTRAS>", extras)
        sparql = sparql.replace("<URI>", "<{}>".format(str(target_uri)))
        overlaps = []
        bindings = []
        await query_build_response_bindings(sparql, count, offset, bindings)
        extras = ""
        if include_contains:
            use_selects = selects
            if use_areas_sparql:
                extras = areas_sparql
                use_selects += area_selects
            sparql = contains_sparql.replace("<SELECTS>", use_selects)
            sparql = sparql.replace("<EXTRAS>", extras)
            sparql = sparql.replace("<URI>", "<{}>".format(str(target_uri)))
            await query_build_response_bindings(sparql, count, offset, bindings)
            extras = ""
        if include_within:
            use_selects = selects
            if use_areas_sparql:
                extras = areas_sparql
                use_selects += area_selects
            sparql = within_sparql.replace("<SELECTS>", use_selects)
            sparql = sparql.replace("<EXTRAS>", extras)
            sparql = sparql.replace("<URI>", "<{}>".format(str(target_uri)))
            await query_build_response_bindings(sparql, count, offset, bindings)
        if len(bindings) < 1:
            return {'count', 0}, overlaps
        if not include_proportion and not include_areas:
            my_area = False
            for b in bindings:
                overlaps.append(b['o']['value'])
        else:
            d100 = Decimal("100.0")
            try:
                uarea = bindings[0]['uarea']
            except (LookupError, AttributeError):
                raise ReportableAPIError("Source feature does not have a known geometry area."
                                         "Cannot return areas or calculate proportions.")
            my_area = round(Decimal(uarea['value']), 8)
            for b in bindings:
                o_dict = {"uri": b['o']['value']}
                if include_within:
                    try:
                        is_w = b['w']
                    except (LookupError, AttributeError):
                        is_w = False
                    o_dict["isWithin"] = bool(is_w)
                if include_contains:
                    try:
                        has_c = b['c']
                    except (LookupError, AttributeError):
                        has_c = False
                    o_dict["contains"] = bool(has_c)
     
                overlaps.append(o_dict)
                try:
                    oarea = b['oarea']
                except (LookupError, AttributeError):
                    continue
                o_area = round(Decimal(oarea['value']), 8)
                if include_areas:
                    o_dict['featureArea'] = str(o_area)
                if include_proportion:
                    if include_within and is_w:
                        my_proportion = d100
                        other_proportion = (my_area / o_area) * d100
                        i_area = my_area
                    elif include_contains and has_c:
                        my_proportion = (o_area / my_area) * d100
                        other_proportion = d100
                        i_area = o_area
                    else:
                        try:
                            i_area = Decimal(b['iarea']['value'])
                        except (LookupError, AttributeError):
                            continue
                        my_proportion = (i_area / my_area) * d100
                        other_proportion = (i_area / o_area) * d100
                    if include_areas:
                        o_dict['intersectionArea'] = str(round(i_area, 8))
                    my_proportion = round(my_proportion, 8)
                    other_proportion = round(other_proportion, 8)
                    o_dict['forwardProportion'] = str(my_proportion)
                    o_dict['reverseProportion'] = str(other_proportion)


        meta = {
            'count': len(overlaps),
            'offset': offset,
        }
        if my_area and include_areas:
            meta['featureArea'] = str(my_area)
            
        # End of original SPARQL code
        
    return meta, overlaps

async def get_at_location(lat, lon, loci_type="any", count=1000, offset=0):
    """
    :param lat:
    :type lat: float 
    :param lon:
    :type lon: float 
    :param count:
    :type count: int
    :param offset:
    :type offset: int
    :return:
    """
    if get_at_location.pool is None:
        get_at_location.pool = await asyncpg.create_pool('postgresql://postgres:password@{}:5437/mydb'.format(GEOBASE_ENDPOINT), command_timeout=60, min_size=1, max_size=2)
    conn = await get_at_location.pool.acquire() 
    row = {} 
    results = {} 
    counter = 0
    try:
        if loci_type == 'mb' or loci_type == 'any': 
            row = await conn.fetchrow(
                    'select mb_code_20 from "from" where ST_Intersects(ST_Transform(ST_GeomFromText(\'POINT(\' || $1 || \' \' || $2 || \')\', 4326),3577), "from".geom_3577) order by mb_code_20 limit $3 offset $4', str(lon), str(lat), count, offset)
            if row is not None and len(row) > 0: 
                results["mb"] = ["http://linked.data.gov.au/dataset/asgs2016/meshblock/{}".format(row['mb_code_20'])]
                counter += len(row)
        if loci_type == 'cc' or loci_type == 'any':
            row = await conn.fetchrow(
                    'select hydroid from "to" where ST_Intersects(ST_Transform(ST_GeomFromText(\'POINT(\' || $1 || \' \' || $2 || \')\', 4326),3577), "to".geom_3577) order by hydroid limit $3 offset $4', str(lon), str(lat), count, offset)
            if row is not None and len(row) > 0: 
                results["cc"] = ["http://linked.data.gov.au/dataset/geofabric/contractedcatchment/{}".format(row['hydroid'])]
                counter += len(row)
    finally:
        await get_at_location.pool.release(conn)
    meta = {
        'count': counter,
        'offset': offset,
    }
    return meta, results
get_at_location.pool = None 
       
async def query_es_endpoint(query, limit=10, offset=0):
    """
    Pass the ES query to the endpoint. The endpoint is specified in the config file.

    :param query: the query text
    :type query: str
    :param limit:
    :type limit: int
    :param offset:
    :type offset: int
    :return:
    :rtype: dict
    """
    loop = asyncio.get_event_loop()
    http_ok = [200]
    try:
        session = query_es_endpoint.session_cache[loop]
    except KeyError:
        session = ClientSession(loop=loop)
        query_es_endpoint.session_cache[loop] = session
    args = {
        'q': query
#        'limit': int(limit),
#        'offset': int(offset),
    }
    
    formatted_resp = {
        'ok': False
    }
    try:
        resp = await session.request('GET', ES_ENDPOINT, params=args)
        resp_content = await resp.text()
        if resp.status not in http_ok:
            formatted_resp['errorMessage'] = "Could not connect to the label search engine. Error code {}".format(resp.status)
            return formatted_resp
        formatted_resp = loads(resp_content)
        formatted_resp['ok'] = True
        return formatted_resp
    except ClientConnectorError:
        formatted_resp['errorMessage'] = "Could not connect to the label search engine. Connection error thrown."
        return formatted_resp       
    return formatted_resp
query_es_endpoint.session_cache = {}


async def search_location_by_label(query):
    """
    Query ElasticSearch endpoint and search by label of LOCI locations. 
    The query to ES is in the format of http://localhost:9200/_search?q=NSW

    Returns response back from ES as-is.

    :param query: query string for text matching on label of LOCI locations
    :type query: str
    :return:
    :rtype: dict
    """
    resp = await query_es_endpoint(query)
    
    if ('ok' in resp and resp['ok'] == False):
        return resp
    
    resp_object = {}
    if 'hits' not in resp:        
        return resp_object
    
    resp_object = resp
    return resp_object   

