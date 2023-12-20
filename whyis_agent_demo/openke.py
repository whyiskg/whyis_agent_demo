import flask
from whyis import autonomic
import rdflib
from slugify import slugify
from whyis import nanopub
import collections

from uuid import uuid4

from transformers import pipeline

from whyis.namespace import NS as ns
schema = ns.schema

wd = rdflib.Namespace('http://www.wikidata.org/entity/')

import spacy
from spacy_experimental.coref.coref_component import DEFAULT_COREF_MODEL
from spacy_experimental.coref.coref_util import DEFAULT_CLUSTER_PREFIX

test_text = '''Monstera deliciosa, the Swiss cheese plant or split-leaf philodendron is a species of flowering plant native to tropical forests of southern Mexico, south to Panama. It has been introduced to many tropical areas, and has become a mildly invasive species in Hawaii, Seychelles, Ascension Island and the Society Islands. It is very widely grown in temperate zones as a houseplant.'''

def sentences(text):
    nlp = spacy.load("en_core_web_lg")
    config={
        "model": DEFAULT_COREF_MODEL,
        "span_cluster_prefix": DEFAULT_CLUSTER_PREFIX,
    }
    nlp.add_pipe("experimental_coref", config=config)


class OpenKnowledgeExtractor(autonomic.UpdateChangeService):
    activity_class = wd.Q1582085
    _extractor = None

    properties = {
        "instance_of" : ns.RDF.type,
        "subclass_of" : ns.RDFS.subClassOf,
    }
    types = {
        "per": wd.Q215627, # Person
        "loc": wd.Q2221906, # Location -> geographical location
        "org" : wd.Q43229, # Organization (org)
        "eve" : wd.Q1190554, # Event (eve) -> occurrence
        "bio" : wd.Q28845870, # Biology (bio) -> biological component

        "concept" : ns.skos.Concept, # or wd:Q151885
        "anim" : wd.Q16521, # Animal (anim) -> taxon
        "misc" : wd.Q35120, # -> entity
        "dis" : wd.Q12136, # Disease (dis),
        "food" : wd.Q2095, # Food (food),
        "inst" : wd.Q39546, # Instrument (inst), -> tool
        "media" : wd.Q17537576, # Media (media), -> creative work
        "mon" : wd.Q1499548, # Monetary (mon), -> monetary value
        "num" : wd.Q11563, # Number (num),
        "phys" : wd.Q1293220, # Physical Phenomenon (phys),
        "plant" : wd.Q16521, # Plant (plant), -> taxon
        "super" : wd.Q28855038, # Supernatural (super), -> supernatural being
        "date" : wd.Q205892, # Date (date), -> calendar date
        "time" : wd.Q1260524, # Time (time), -> time of day
        "vehi" : wd.Q29048322, # Vehicle (vehi) -> vehicle model
    }


    def getInputClass(self):
        return schema.MediaObject

    def getOutputClass(self):
        return ns.whyis.KnowledgeExtractedMedia

    def get_query(self):
        return '''select distinct ?resource where { ?resource schema:text|schema:caption|schema:description [].}'''

    @property
    def extractor(self):
        if self._extractor is None:
            self._extractor = pipeline(
                'translation_xx_to_yy',
                model='Babelscape/mrebel-large',
                tokenizer='Babelscape/mrebel-large'
            )
        return self._extractor

    def extract(self, text, language='en_XX'):
        tokens = self.extractor(text,
                                decoder_start_token_id=250058,
                                max_length = len(text),
                                src_lang=language,
                                tgt_lang="<triplet>",
                                return_tensors=True,
                                return_text=False)
        extracted_text = self.extractor.tokenizer.batch_decode([tokens[0]["translation_token_ids"]])
        triples = self.parse_typed_triples(extracted_text[0])
        return triples

    def parse_typed_triples(self, text):
        triplets = []
        relation = ''
        text = text.strip()
        current = 'x'
        subject, relation, object_, object_type, subject_type = '','','','',''

        for token in text.replace("<s>", "").replace("<pad>", "").replace("</s>", "").replace("tp_XX", "").replace("__en__", "").split():
            if token == "<triplet>" or token == "<relation>":
                current = 't'
                if relation != '':
                    triplets.append({'head': subject.strip(), 'head_type': subject_type, 'type': relation.strip(),'tail': object_.strip(), 'tail_type': object_type})
                    relation = ''
                subject = ''
            elif token.startswith("<") and token.endswith(">"):
                if current == 't' or current == 'o':
                    current = 's'
                    if relation != '':
                        triplets.append({'head': subject.strip(), 'head_type': subject_type, 'type': relation.strip(),'tail': object_.strip(), 'tail_type': object_type})
                    object_ = ''
                    subject_type = token[1:-1]
                else:
                    current = 'o'
                    object_type = token[1:-1]
                    relation = ''
            else:
                if current == 't':
                    subject += ' ' + token
                elif current == 's':
                    object_ += ' ' + token
                elif current == 'o':
                    relation += ' ' + token
        if subject != '' and relation != '' and object_ != '' and object_type != '' and subject_type != '':
            triplets.append({
                'head': subject.strip(),
                'head_type': subject_type,
                'type': relation.strip(),
                'tail': object_.strip(),
                'tail_type': object_type
            })
        return triplets

    def process(self, i, o):
        lod_prefix = flask.current_app.config['LOD_PREFIX']

        entity_ns = rdflib.Namespace(lod_prefix + '/entity/')
        property_ns = rdflib.Namespace(lod_prefix + '/property/')
        type_ns = rdflib.Namespace(lod_prefix + '/class/')

        entities = collections.defaultdict(lambda: entity_ns[str(uuid4())])
        # TODO: look up already generated entities associated with this media

        for text in i[schema.text|schema.description|schema.caption]:
            paragraphs = text.value.split('\n\n')
            for para in paragraphs:
                triples = self.extract(para)
                for triple in triples:
                    subject_label = triple['head']
                    subj = entities[subject_label]
                    subj_type = self.types.get(triple['head_type'], type_ns[triple['head_type']])
                    subj_resource = o.graph.resource(subj)
                    subj_resource.add(ns.RDFS.label, rdflib.Literal(subject_label))
                    subj_resource.add(ns.RDF.type, subj_type)
                    o.add(ns.schema.about, subj_resource)

                    object_label = triple['tail']
                    obj = entities[object_label]
                    obj_type = self.types.get(triple['tail_type'], type_ns[triple['tail_type']])
                    obj_resource = o.graph.resource(obj)
                    obj_resource.add(ns.RDFS.label, rdflib.Literal(object_label))
                    obj_resource.add(ns.RDF.type, obj_type)
                    o.add(ns.schema.about, obj_resource)

                    property_label = triple['type']
                    property_local = property_label.replace(' ','_')
                    prop = self.properties.get(property_local,property_ns[property_local])
                    prop_resource = o.graph.resource(prop)
                    prop_resource.add(ns.RDFS.label, rdflib.Literal(property_label))

                    o.graph.add((subj, prop, obj))
                    # Alternately:
                    subj_resource.add(prop, obj)
