# This is my starting point. Here, I'm writing the code I want to be able to 
# write in a Jupyer notebook, that helps guide me in developing my data pipeline
#%%
from etl import DataPipeline, WikipediaPipelineStep
import xml.etree.ElementTree as xml
from warnings import warn
import re
import asyncio

#%% Create the pipeline
pipeline = DataPipeline()

#%%
class NormalizeCityNames(WikipediaPipelineStep):
    """First step: take incoming city names and normalize them according to the titles of
    their pages on Wikipedia.

    Changes batch size to 50 since Wikipedia supports _querying_ 50 pages at a time
    """

    batch_size = 50

    async def process_batch(self, city_names):
        params = {
            "action": "query",
            "format": "json",
            "redirects": 1,
            "titles": "|".join(city_names)
        }
        response = await self.make_request(params)['query']
        for redirect in response['redirects']:
            #TODO: Cache redirects
            yield redirect['to']
        for page in response['pages'].values():
            if "missing" in page.keys():
                warn(f"The city {page['title']} was provided but is not available on Wikipedia, and has been skipped")
            if page['title'] in city_names:
                # Original name was not redirected; yield the original name
                yield page['title']
# %%
class ParseTree(WikipediaPipelineStep):
    """
    For each incoming city name, attatch its Wikipedia parsetree
    """
    async def process_batch(self, normalized_city_names):
        for city in normalized_city_names: # TODO: Can/should I make this an async iterator?
            params = {
                "action": "parse",
                "format": "json",
                "redirects": 1,
                "prop": "parsetree",
                "page": city
            }
            raw_response = await self.make_request(params)['parse']['parsetree']['*']
            response = xml.canonicalize(raw_response, strip_text=True)
            yield (city, xml.fromstring(response))

#%%
class GetCitiesFromCounties(WikipediaPipelineStep):
    """
    Figure out which of a city's navboxes is for the county, and use that to expand each
    city into the cities in its county.

    Leaves city and tree attached so that the tree for the original city is not obtained twice.
    """
    async def process_batch(self, city_parsetrees):
        for original_city, tree in city_parsetrees:
            for template in self.get_navbox_templates(tree):
                raw_response, template_page = await self.get_template_page(template)
                root = template_page.find(".//template[title='US county navigation box']")
                if root == None:
                    continue
                seat = await self.get_seat(root)
                cities = self.parse_cities(raw_response)
                for city in cities:
                    yield (original_city, tree, city, seat)
                break


    def get_navbox_templates(self, wiki_page_tree):
        navboxes = wiki_page_tree.findall(".//template[title='Navboxes']/part[name='list']/value/template/title")
        return ['Template:{}'.format(elem.text) for elem in navboxes]

    async def get_template_page(self, template):
        params = {
            "action": "parse",
            "format": "json",
            "redirects": 1,
            "prop": "parsetree",
            "page": template
        }
        raw_response = await self.make_request(params)['parse']['parsetree']['*']
        response = xml.canonicalize(raw_response, strip_text=True)
        return raw_response, xml.fromstring(response)
        
    async def get_seat(self, root):
        seat_name = root.find(".//part[name='seat']/value").text
        params = {
            "action": "query",
            "format": "json",
            "redirects": "1",
            "page": seat_name
        }
        page = self.make_request(params)['query']['pages'].values()[0]
        assert 'missing' not in page
        return page['title']

        
    async def parse_cities(self, raw_response_txt):
        listed_city = re.compile(r"""
            ^\* \ * # Line starts with "*" plus any number of spaces
            \[{2} # Start of link "[["
                ([^\|]+) # First part of link (between "[[" and "|"). This is the part that gets captured.
                \| # Separator "|"
                [^\|]+ # Second part of link (between "|" and "]]")
            \]{2}‡? # End of link "]]" plus optional ‡ character
            \ *$""", re.VERBOSE + re.MULTILINE
        )
        return listed_city.findall(raw_response_txt)


#%%
pipeline = DataPipeline([
    NormalizeCityNames(),
    ParseTree(),
    GetCitiesFromCounties()
])
pipeline.run()