from PIL import Image, ImageOps
import requests
from io import BytesIO
import pytesseract
import collections
import re

import flask
from whyis import autonomic
import rdflib
from slugify import slugify
from whyis import nanopub
import collections

from whyis.namespace import NS as ns

schema = ns.schema
wd = rdflib.Namespace('http://www.wikidata.org/entity/')

class OCRCaptioner(autonomic.UpdateChangeService):
    activity_class = wd.Q167555 # optical character recognition

    def getInputClass(self):
        return ns.schema.ImageObject

    def getOutputClass(self):
        return ns.whyis.CaptionedImage

    def get_query(self):
        return '''select distinct ?resource where { ?resource a <http://schema.org/ImageObject>. }'''

    def load_image(self, location):
        resource = flask.current_app.get_resource(location, async_=False)
        fileid = resource.value(flask.current_app.NS.whyis.hasFileID)
        if fileid is not None:
            return Image.open(flask.current_app.file_depot.get(fileid.value))
        else:
            r = requests.get(location)
            return Image.open(BytesIO(r.content))

    def extract_text(self, image):
        #image = ImageOps.autocontrast(image)
        image = ImageOps.scale(image, 4, resample=Image.Resampling.LANCZOS)
        image = ImageOps.grayscale(image)

        boxes = pytesseract.image_to_data(image, output_type=pytesseract.Output.DATAFRAME, config='--psm 4')
        blocks = collections.defaultdict(list)

        for i, box in boxes.iterrows():
            if box.conf > 0:# and re.search('[0-9a-zA-Z\s]+',box.text):
                blocks[box.block_num].append(box.text)

        text = '\n'.join([' '.join(blocks[k]) for k in sorted(blocks.keys())])
        return text

    def process(self, i, o):
        image = self.load_image(i.identifier)
        text = self.extract_text(image).strip() # "This is a giraffe."
        o.add(schema.caption, rdflib.Literal(text))

    def process_nanopub(self, i, o, nanpub):
        image = self.load_image(i.identifier)
        text = self.extract_text(image).strip() # "This is a giraffe."
        o.add(schema.caption, rdflib.Literal(text))
