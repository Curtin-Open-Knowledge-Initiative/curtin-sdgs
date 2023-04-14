"""
Checking the OSDG-MAG mapping vs the set of OpenAlex concepts that are available in that dataset
"""

import json

with open("external_data/osdg_mag._mapping.json") as f:
    osdg = json.load(f)

with open('external_data/openalex_concepts.json') as f:
    oac = json.load(f)

oa_mags = set([concept.get("mag") for concept in oac])
oa_mags_index = {concept["mag"]: concept for concept in oac }
matches = {}
for sdg in osdg.keys():
    print(sdg)
    mmags = set(osdg[sdg])
    lost = mmags - oa_mags
    common = mmags & oa_mags
    levels = {i: 0 for i in range(7)}
    for concept in common:
        levels[int(oa_mags_index[concept]["level"])] += 1

    lost_count = len(lost)
    print(f"Mapped MAG IDs: {len(mmags)}")
    print(f"Lost MAG Ids: {lost_count}")
    for level in range(7):
        print(f"Common at level {level}: {levels.get(level)}")