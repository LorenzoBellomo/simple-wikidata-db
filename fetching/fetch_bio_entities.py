
from tqdm import tqdm
from multiprocessing import Pool
from functools import partial
import numpy as np
import pandas as pd
import re
import json

from utils import jsonl_generator, get_batch_files

#INSTANCE OF, SUBCLASS OF, TUTTE LE WIKI CAT IN CATEGORIE

with open("/raid/wikidata/bio_mapping.json", "r") as json_file:
    cat_mapping = json.load(json_file)

cat_keys = list(cat_mapping.keys())
processed_dir = "/raid/wikidata/bio_processed/"
full_preprocessed_dir = "/raid/wikidata/processed/"

def parallel_exec(funct, filename):
    table_files = get_batch_files(processed_dir + filename)
    pool = Pool(processes=10)
    filtered = []
    for output in tqdm(pool.imap_unordered(partial(funct), table_files, chunksize=1), total=len(table_files)):
        filtered.extend(output)
    print(f"Extracted {len(filtered)} rows:")
    return filtered

def parallel_exec_full(funct, filename, args):
    table_files = get_batch_files(full_preprocessed_dir + filename)
    pool = Pool(processes=10)
    filtered = []
    for output in tqdm(pool.imap_unordered(partial(funct, args), table_files, chunksize=1), total=len(table_files)):
        filtered.extend(output)
    print(f"Extracted {len(filtered)} rows:")
    return filtered

def parallel_exec_arg(funct, filename, args):
    table_files = get_batch_files(processed_dir + filename)
    pool = Pool(processes=10)
    filtered = []
    for output in tqdm(pool.imap_unordered(partial(funct, args), table_files, chunksize=1), total=len(table_files)):
        filtered.extend(output)
    print(f"Extracted {len(filtered)} rows:")
    return filtered

def get_wikipedias(filename):
    filtered = []
    for item in jsonl_generator(filename):
        filtered.append((item['qid'], item['wiki_title']))
    return filtered

def get_titles(filename):
    filtered = []
    for item in jsonl_generator(filename):
        filtered.append((item['qid'], item['label']))
    return filtered

def get_aliases(filename):
    filtered = []
    for item in jsonl_generator(filename):
        filtered.append((item['qid'], item['alias']))
    return filtered

def get_main_category(filename):
    filtered = []
    for item in jsonl_generator(filename):
        if item["property_id"] == "P31" and item["value"] in cat_keys:
            filtered.append((item['qid'], item['value']))
    return filtered

def get_pagelinks(filename):
    filtered = []
    for item in jsonl_generator(filename):
        filtered.append((item['qid'], item['value']))
    return filtered

def get_categories(filename):
    filtered = []
    for item in jsonl_generator(filename):
        if item["property_id"] == "P31" and item["value"] not in cat_keys:
            filtered.append((item['qid'], item['value']))
        elif item["property_id"] == "P279" and item["value"] not in cat_keys:
            filtered.append((item['qid'], item['value']))
    return filtered

def get_cat_titles(categories, filename):
    filtered = []
    for item in jsonl_generator(filename):
        if item['qid'] in categories:
            filtered.append((item['qid'], item['label']))
    return filtered


def main():
    titles = parallel_exec(get_titles, "labels")
    aliases = parallel_exec(get_aliases, "aliases")
    main_cats = parallel_exec(get_main_category, "entity_rels")
    categories = parallel_exec(get_categories, "entity_rels")
    all_ids = [a for a, _ in titles]
    cats = set()

    # columns_ = ['id','main_category', 'title', 'aliases', 'categories', 'wiki_titles', 'bio_id']
    all_data = {k: {'aliases': [], 'categories': []} for k in all_ids}
    for qid, a in titles:
        all_data[qid]['title'] = a
    for qid, a in aliases:
        if qid in all_data:
            all_data[qid]['aliases'].append(a)
        else:
            all_data[qid] = {'title': a, 'aliases': [], 'categories': []}
    for qid, a in main_cats:
        if qid in all_data:
            cats.add(a)
            all_data[qid]['main_cat'] = a
    for qid, a in categories:
        if qid in all_data:
            cats.add(a)
            all_data[qid]['categories'].append(a)
    

    pagelinks = parallel_exec(get_pagelinks, "entity_rels")
    title_cats = parallel_exec_full(get_cat_titles, "labels", list(cats))
    cat_mapping_titles = {k: v for k, v in title_cats}

    with open("../../for_ontotagme/page.csv", 'w') as page_file:
        for qid, info in all_data.items():
            page_file.write(info['title'] + "\t" + str(qid) + "\n") 
            for alias in info['aliases']:
                if alias != info['title']:
                    page_file.write(alias + "\t" + qid + "\t" + str(all_data[qid]['title']) + "\n")
                if "(" in alias:
                    # CASE like: "ATP (Molecule)", add also "ATP" to the redirects
                    new_alias = re.sub(r"[\(].*?[\)]", "", alias).strip()
                    if new_alias != alias:
                        page_file.write(new_alias + "\t" + qid + "\t" + str(all_data[qid]['title']) + "\n")

    with open("../../for_ontotagme/category.csv", 'w') as cat_file:
        for qid, info in all_data.items():
            categories_str = ";".join([cat_mapping_titles.get(a, "NO TITLE") for a in list(info['categories'] + [info['main_cat']])])
            cat_file.write(info['title']+ '\t' + qid + "\t" + categories_str + "\n")
            for alias in info['aliases']:
                cat_file.write(alias + '\t' + qid + "\t" + categories_str + "\n")
    
    with open("../../for_ontotagme/pagelinks.csv", 'w') as pagelinks_file:
        for from_id, to_id in pagelinks:
            if from_id in all_data and to_id in all_data:
                title_from = all_data[from_id]['title']
                title_to = all_data[to_id]['title']
                pagelinks_file.write(title_from + "\t" + title_to + "\n") 

if __name__ == "__main__":
    main()