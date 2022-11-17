# ALIASES
- qid: item ID
- alias: synonym

# DESCRIPTIONS
- qid: item ID
- description: First line of the page, describing it

# ENTITY_RELS
LOOK at "Statements" table on any Wikidata page
- claim_id: useless, row ID
- qid: item ID
- property_id: ID of the property on the left of the statements table ("instance of" for example)
- value: the page which this property refers to

# ENTITY_VALUES
LOOK at "Statements" table on any Wikidata page, similar to table above, but without a link to a Wikidata page
- claim_id: useless, row ID (useful only if you need the qualifiers, see below)
- qid: item ID
- property_id: ID of the property on the left of the statements table ("hashtag" for example)
- value: the value of this property (#happy, a link to the Wikimedia commons of the category page and such)

# EXTERNAL_IDS
LOOK at "Identifiers" table on any Wikidata page
- claim_id: useless, row ID
- qid: item ID
- property_id: ID of the property on the left of the identifiers table ("National Library of Israel J9U ID")
- value: the  id used in this case

# LABELS
- qid: item ID
- label: title

# QUALIFIERS
LOOK at "Statements" table on any Wikidata page, sometimes, together with the claim
- claim_id: links to the claim in entity_rels
- qualifier_id: useless
- property_id: the property further specified for this claim
- value: the value

# WIKIPEDIA_LINKS
- qid: item ID
- wiki_title: title of the wikipedia page

