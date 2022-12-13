
from tqdm import tqdm
from multiprocessing import Pool
from functools import partial
import numpy as np
import pandas as pd
import re
import json
from collections import defaultdict
import random

from utils import jsonl_generator, get_batch_files

#INSTANCE OF, SUBCLASS OF, TUTTE LE WIKI CAT IN CATEGORIE

with open("../bio_mapping.json", "r") as json_file:
    cat_mapping = json.load(json_file)
blacklist_page = []
blacklist_redirect_tmp = defaultdict(list)
with open("../page-redirect_delete.txt", "r") as blacklist_file:
    for line in blacklist_file.readlines():
        splt = line.replace("\n", "").strip().split(",")
        if len(splt) == 2:
            blacklist_redirect_tmp[splt[0]].append(splt[1])
        else:
            blacklist_page.append(splt[0])

blacklist_redirect = dict(blacklist_redirect_tmp)

cat_keys = list(cat_mapping.keys())
with open("../config.json", 'r') as json_file:
    config = json.load(json_file)
    full_preprocessed_dir = config['FULL_PREPROCESSED_URL']
    processed_dir = config['BIO_PREPROCESSED_URL']
    ontotagme_dir = config["FOR_ONTOTAGME_URL"]
    external_dir = config['EXTERNAL_IDS_PROCESSED']

with open("../external_id_mapping.json", "r") as json_file:
    mapping_type_id = json.load(json_file)

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

def parallel_exec_ext_ids(funct, filename, args):
    table_files = get_batch_files(external_dir + filename)
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

def get_pagelinks(filename):
    filtered = []
    for item in jsonl_generator(filename):
        filtered.append((item['qid'], item['value']))
    return filtered

def get_categories(filename):
    filtered = []
    for item in jsonl_generator(filename):
        if item["property_id"] == "P31":
            filtered.append((item['qid'], item['value']))
            if item['value'] in cat_keys:
                filtered.append((item['qid'], cat_mapping[item['value']]))
        elif item["property_id"] == "P279":
            filtered.append((item['qid'], item['value']))
            if item['value'] in cat_keys:
                filtered.append((item['qid'], cat_mapping[item['value']]))
        elif item["property_id"] == "P361":
            filtered.append((item['qid'], item['value']))
            if item['value'] in cat_keys:
                filtered.append((item['qid'], cat_mapping[item['value']]))
    return filtered

def get_categories_of_external_id_items(which, filename):
    filtered = []
    for item in jsonl_generator(filename):
        if item['qid'] in which:
            if item["property_id"] == "P31":
                filtered.append((item['qid'], item['value']))
            elif item["property_id"] == "P279":
                filtered.append((item['qid'], item['value']))
            elif item["property_id"] == "P361":
                filtered.append((item['qid'], item['value']))
    return filtered

def get_cat_titles(categories, filename):
    filtered = []
    for item in jsonl_generator(filename):
        if item['qid'] in categories:
            filtered.append((item['qid'], item['label']))
    return filtered

def get_external_ids(args, filename):
    filtered = []
    for item in jsonl_generator(filename):
        if item['property_id'] in mapping_type_id:
            id_name = mapping_type_id[item['property_id']]
            full_id = id_name + ":" + item['value']
            filtered.append((item['qid'], full_id))
    return filtered

def get_external_titles(args, filename):
    filtered = []
    for item in jsonl_generator(filename):
        filtered.append((item['qid'], item['label']))
    return filtered


def main():
    titles = parallel_exec(get_titles, "labels")
    aliases = parallel_exec(get_aliases, "aliases")
    categories = parallel_exec(get_categories, "entity_rels")
    external_ids_list = parallel_exec_ext_ids(get_external_ids, "external_ids", None)
    external_ids_title = parallel_exec_ext_ids(get_external_titles, "labels", None)
    external_ids = {}
    for qid, ext_id in external_ids_list:
        external_ids[qid] = {"ext_id": ext_id, 'cats': list(), 'title': "UNKNOWN TITLE"}
    for qid, title in external_ids_title:
        if qid in external_ids:
            external_ids[qid]['title'] = title
    cats = set()
    extra_categories = parallel_exec_ext_ids(get_categories_of_external_id_items, "entity_rels", list(external_ids.keys()))
    j = 0
    for qid, cat_id in extra_categories:
        cats.add(cat_id)
        external_ids[qid]['cats'].append(cat_id)
    print(j, "-", len(extra_categories))
    all_ids = [a for a, _ in titles]

    all_data = {k: {'aliases': [], 'categories': []} for k in all_ids}
    for qid, a in titles:
        all_data[qid]['title'] = a
    for qid, a in aliases:
        if qid in all_data:
            all_data[qid]['aliases'].append(a)
        else:
            all_data[qid] = {'title': a, 'aliases': [], 'categories': []}
    for qid, a in categories:
        if qid in all_data:
            cats.add(a)
            all_data[qid]['categories'].append(a)
    

    pagelinks = parallel_exec(get_pagelinks, "entity_rels")
    title_cats = parallel_exec_full(get_cat_titles, "labels", list(cats))
    cat_mapping_titles = {k: v for k, v in title_cats}

    with open("{}page.csv".format(ontotagme_dir), 'w') as page_file:
        for qid, info in all_data.items():
            if qid in blacklist_page:
                continue
            page_file.write(info['title'] + "\t" + str(qid) + "\n") 
            for alias in info['aliases']:
                if qid not in blacklist_redirect.keys() or alias not in blacklist_redirect[qid]:
                    if alias != info['title']:
                        page_file.write(alias + "\t" + qid + "\t" + str(all_data[qid]['title']) + "\n")
                    if "(" in alias:
                        # CASE like: "ATP (Molecule)", add also "ATP" to the redirects
                        new_alias = re.sub(r"[\(].*?[\)]", "", alias).strip()
                        if new_alias != alias:
                            page_file.write(new_alias + "\t" + qid + "\t" + str(all_data[qid]['title']) + "\n")

    with open("{}category.csv".format(ontotagme_dir), 'w') as cat_file:
        for qid, info in all_data.items():
            all_cats = set([cat_mapping_titles.get(a, "NO TITLE") for a in list(info['categories'])])
            for y in [cat_mapping[x] for x in info['categories'] if x in cat_mapping.keys()]:
                all_cats.add(y)
            categories_str = ";".join(list(all_cats))
            cat_file.write(info['title']+ '\t' + qid + "\t" + categories_str + "\n")
            for alias in info['aliases']:
                cat_file.write(alias + '\t' + qid + "\t" + categories_str + "\n")
    
    with open("{}pagelinks.csv".format(ontotagme_dir), 'w') as pagelinks_file:
        for from_id, to_id in pagelinks:
            if from_id in all_data and to_id in all_data:
                title_from = all_data[from_id]['title']
                title_to = all_data[to_id]['title']
                pagelinks_file.write(title_from + "\t" + title_to + "\n") 
        keys_ = list(all_data.keys())
        for qid, info in all_data.items():
            title_from = all_data[qid]['title']
            for to_id in random.sample(keys_, 5):
                title_to = all_data[to_id]['title']
                pagelinks_file.write(title_from + "\t" + title_to + "\n") 

    with open("{}external_ids.csv".format(ontotagme_dir), 'w') as write_file:
        j = 0
        for qid, v in external_ids.items():
            category_to_str = [cat_mapping_titles.get(c, "NO TITLE") for c in v['cats']]
            categories_str = ";".join([a for a in category_to_str if a != "NO TITLE"])
            # QID   \t    EXTERNAL_ID   \t   cat1;cat2;cat3 \n  (note that external ID is already formatted as bert wants) 
            write_file.write(qid + "\t" + v['title'] + "\t" + v['ext_id'] + "\t" + categories_str + "\n")
            j = j + 1
    print("tot number of external IDs", j)

if __name__ == "__main__":
    main()