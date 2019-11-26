# -*- coding: utf-8 -*-
#
import sys
module = sys.modules[__name__]
CONFIG = module.CONFIG = {}
#TRIPLESTORE_CACHE_URL = CONFIG["TRIPLESTORE_CACHE_URL"] = "http://db.loci.cat"
TRIPLESTORE_CACHE_URL = CONFIG["TRIPLESTORE_CACHE_URL"] = "http://ec2-52-64-179-154.ap-southeast-2.compute.amazonaws.com"
TRIPLESTORE_CACHE_PORT = CONFIG["TRIPLESTORE_CACHE_PORT"] = "80"
TRIPLESTORE_CACHE_SPARQL_ENDPOINT = CONFIG["TRIPLESTORE_CACHE_SPARQL_ENDPOINT"] = \
    "{}:{}/repositories/loci-cache".format(TRIPLESTORE_CACHE_URL, TRIPLESTORE_CACHE_PORT)
GEOBASE_ENDPOINT = CONFIG["GEOBASE_ENDPOINT"] = "10.0.1.200"

ES_URL = CONFIG["ES_URL"] = "http://elasticsearch"
ES_PORT = CONFIG["ES_ENDPOINT"] = "9200"
ES_ENDPOINT = CONFIG["ES_ENDPOINT"] = \
    "{}:{}/_search".format(ES_URL, ES_PORT)

USE_SQL=True
    
DB_CONFIG = {
"POSTGRES_SERVER": "localhost",
"POSTGRES_PORT": 5432,
"POSTGRES_DBNAME": "loci_test",
"POSTGRES_USER": "loci",
"POSTGRES_PASSWORD": "loci",
"AUTOCOMMIT": True,
}

