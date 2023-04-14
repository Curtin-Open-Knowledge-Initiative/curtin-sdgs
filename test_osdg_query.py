import jinja2
import json
from report_data_processing.sql import load_sql_to_string
from parameters import *

with open("external_data/osdg_mag._mapping.json") as f:
    osdg = json.load(f)

with open('external_data/openalex_concepts.json') as f:
        oac = json.load(f)

oa_mags = set([concept.get("mag") for concept in oac])
oa_mags_index = {concept["mag"]: concept for concept in oac }
sdgs = []
for sdg in osdg.keys():
    mmags = set(osdg[sdg])
    common = mmags & oa_mags
    sdgs.append(dict(
        name=sdg,
        oa_concepts = '"'+ '","'.join([oa_mags_index[mag]["id"] for mag in common]) + '"'
    ))

query_template = load_sql_to_string('osdgs.sql.jinja2',
                                    directory=SQL_DIRECTORY)

data = dict(
    table=DOI_TABLE,
    sdgs=sdgs
)

query = jinja2.Template(query_template).render(data)
with open('test_query.sql', 'w') as f:
    f.write(query)

